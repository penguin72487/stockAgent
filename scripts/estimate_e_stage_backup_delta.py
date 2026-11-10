#!/usr/bin/env python3
"""Estimate one verified E generation's chunk reuse against a fixed D snapshot.

This is a read-only planning observation, not D object verification, a backup,
current-source freshness or permission to delete anything. The optional D
catalog is parsed incrementally; it is not loaded into memory.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import stat
import time

import ijson

from downloader.artifact_io import atomic_write_json
from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.packed_snapshots import _iter_inventory, resolve_packed_snapshot_id

CONTRACT = 'stockagent.e_stage_backup_delta_estimate.v1'
COLD = Path('/srv/stockagent-packed')
BACKUP_CATALOG = Path('/var/lib/stockagent/backup-stream/catalog.json')
MIB = 1024**2


def check_fixed_chunk_layout(entry: dict) -> int:
    """Admit only layouts whose publication routing this estimator models."""
    chunk_bytes = entry.get('file_chunk_bytes')
    loose_mib = entry.get('loose_threshold_mib')
    if (entry.get('reader_contract') != 'packed-v2-chunks' or type(chunk_bytes) is not int
            or not 1 <= chunk_bytes <= 64 * MIB or entry.get('file_chunking', 'fixed-v1') != 'fixed-v1'
            or type(loose_mib) is not int or loose_mib * MIB != chunk_bytes):
        raise ValueError('this estimate requires fixed chunks with the same explicit loose-file threshold')
    return chunk_bytes


def is_chunked_change(size: int, chunk_bytes: int) -> bool:
    # The publisher stores exactly-threshold files as loose blobs, not packs.
    if size == chunk_bytes:
        raise ValueError('changed file exactly at the loose threshold needs separate blob accounting')
    return size > chunk_bytes


def _signature(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_uid, info.st_gid, info.st_nlink)


def _safe_parts(relative: str) -> tuple[str, ...]:
    p = PurePosixPath(relative)
    if (not relative or p.is_absolute() or not p.parts or '..' in p.parts
            or p.as_posix() != relative or '\\' in relative):
        raise ValueError('invalid fixed inventory member path')
    return p.parts


def hash_chunked_file(root: Path, relative: str, expected_sha: str, size: int, chunk_bytes: int) -> dict:
    """Fully hash one current E file and its ordered chunks under no-follow FDs."""
    parts = _safe_parts(relative)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = child
        file_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
        with os.fdopen(file_fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                raise ValueError('E member type or size changed')
            if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_NOREUSE'):
                try:
                    os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_NOREUSE)
                except OSError:
                    pass
            whole = hashlib.sha256()
            chunks = []
            while block := stream.read(chunk_bytes):
                whole.update(block)
                chunks.append((hashlib.sha256(block).hexdigest(), len(block)))
            after = os.fstat(stream.fileno())
        current = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
        if (_signature(before) != _signature(after) or _signature(after) != _signature(current)
                or whole.hexdigest() != expected_sha or sum(n for _, n in chunks) != size):
            raise ValueError('E member changed or no longer matches its fixed copy SHA')
        return {'size': size, 'chunks': chunks}
    finally:
        os.close(fd)


def find_catalog_matches(stream, candidates: dict[str, int]) -> dict[str, int]:
    """Match the bounded candidates while streaming the large catalog once."""
    found = {}
    for row in ijson.items(stream, 'files.item'):
        if not isinstance(row, dict):
            continue
        digest, size = row.get('sha256'), row.get('bytes')
        relative = row.get('relative')
        if (isinstance(digest, str) and digest in candidates and size == candidates[digest]
                and relative == f'objects/blobs/{digest[:2]}/{digest}.blob'):
            found[digest] = size
    return found


def _indexed_files(path: Path) -> dict[str, tuple[str, int]]:
    if path.is_symlink() or not path.is_file():
        raise ValueError('E inventory is missing or redirected')
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as db:
        db.execute('BEGIN')
        rows = db.execute("SELECT path,sha,bytes FROM files WHERE kind='file' AND state='verified'").fetchall()
    result = {}
    for path_text, digest, size in rows:
        _safe_parts(path_text)
        if (path_text in result or not isinstance(digest, str) or len(digest) != 64
                or any(c not in '0123456789abcdef' for c in digest) or type(size) is not int or size < 0):
            raise ValueError('invalid E indexed file')
        result[path_text] = (digest, size)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', required=True)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--global-catalog', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.workers <= 4:
        parser.error('workers must be between 1 and 4')
    volume.admit(64 * 1024**2)
    workspace = Path(volume.configuration()['mount_point'])
    verify = volume.path('verify')
    output = args.output.absolute()
    comparison_path = args.comparison.absolute()
    if (output.parent != verify or output.exists() or output.is_symlink()
            or comparison_path.parent != verify or comparison_path.is_symlink()):
        parser.error('use new, direct private E verification files')
    progress_path = output.with_suffix('.progress.json')
    if progress_path.exists() or progress_path.is_symlink():
        parser.error('retain the previous progress evidence')
    items = [item for item in queue.load_sources() if item['name'] == args.name]
    if len(items) != 1:
        raise ValueError('source is not uniquely enrolled')
    item = items[0]
    proof = queue.verified_report(item, workspace)
    entries = queue.catalog_entries(item)
    if proof is None or len(entries) != 1 or entries[0].get('publish') is not True:
        raise ValueError('source lacks a public fixed E copy and exact catalog binding')
    entry = entries[0]
    chunk_bytes = check_fixed_chunk_layout(entry)
    comparison = json.loads(comparison_path.read_bytes())
    base = comparison.get('source', {})
    if (comparison.get('contract') != 'stockagent.single_e_stage_d_inventory_comparison.v1'
            or base.get('state') != 'inventory_compared' or base.get('name') != args.name
            or base.get('dataset') != entry.get('dataset') or base.get('staged_files') != proof['files']
            or base.get('staged_bytes') != proof['logical_bytes']
            or comparison.get('retirement_authorized') is not False):
        raise ValueError('comparison does not match this E copy')
    _check_d_primary_mount(COLD)
    fixed = resolve_packed_snapshot_id(COLD, entry['dataset'], base['D_snapshot_id'], require_objects=False)
    manifest = fixed.manifest
    if manifest['archive']['inventory']['sha256'] != base['D_inventory_sha256']:
        raise ValueError('fixed D inventory identity changed')
    base_blobs = {}
    for obj in manifest['archive']['objects']:
        if obj.get('kind') == 'blob':
            digest, size = obj['sha256'], obj['bytes']
            if digest in base_blobs and base_blobs[digest] != size:
                raise ValueError('D baseline contradicts a blob size')
            base_blobs[digest] = size
    _, destination, evidence, _ = queue.paths(item, workspace)
    indexed = _indexed_files(evidence / 'files.sqlite')
    if len(indexed) != proof['files'] or sum(v[1] for v in indexed.values()) != proof['logical_bytes']:
        raise ValueError('E inventory differs from its proof')
    unchanged, changed, small = 0, [], []
    remaining = dict(indexed)
    for row in _iter_inventory(COLD, manifest):
        if row.get('kind') != 'file':
            continue
        relative = row['path']
        value = remaining.pop(relative, None)
        if value is None:
            continue
        if value == (row.get('sha256'), row.get('size')):
            unchanged += 1
        else:
            (changed if is_chunked_change(value[1], chunk_bytes) else small).append((relative, *value))
    for relative, value in remaining.items():
        (changed if is_chunked_change(value[1], chunk_bytes) else small).append((relative, *value))
    expected = base['counts']
    if (unchanged != expected['exact_files'] or len(changed) + len(small) != expected['changed_files'] + expected['e_only_files']
            or sum(row[2] for row in changed + small) != expected['changed_bytes'] + expected['e_only_bytes']):
        raise ValueError('current inventory classification differs from the fixed comparison')
    total_bytes = sum(row[2] for row in changed)
    started = time.monotonic()
    completed_files = completed_bytes = reused_bytes = 0
    unknown: dict[str, int] = {}
    last_progress = 0.0

    def progress(phase: str):
        atomic_write_json(progress_path, {'contract': CONTRACT, 'state': phase,
            'observed_at_utc': datetime.now(UTC).isoformat(), 'completed_files': completed_files,
            'total_files': len(changed), 'completed_bytes': completed_bytes, 'total_bytes': total_bytes,
            'elapsed_seconds': time.monotonic() - started, 'source_files_deleted': 0,
            'D_object_contents_verified': False, 'retirement_authorized': False}, durable=False)

    progress('hashing_fixed_E_files')
    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix='delta-plan') as pool:
        futures = [pool.submit(hash_chunked_file, destination, rel, digest, size, chunk_bytes)
                   for rel, digest, size in changed]
        for future in as_completed(futures):
            value = future.result()
            completed_files += 1
            completed_bytes += value['size']
            for digest, count in value['chunks']:
                if base_blobs.get(digest) == count:
                    reused_bytes += count
                else:
                    if digest in unknown and unknown[digest] != count:
                        raise ValueError('identical chunk digest has inconsistent length')
                    unknown[digest] = count
            if time.monotonic() - last_progress >= 5:
                progress('hashing_fixed_E_files')
                last_progress = time.monotonic()
    if completed_files != len(changed) or completed_bytes != total_bytes:
        raise ValueError('not every selected E file was read')
    progress('checking_existing_D_catalog' if args.global_catalog else 'completed')
    global_found, catalog_generation = {}, None
    if args.global_catalog:
        if BACKUP_CATALOG.is_symlink():
            raise ValueError('backup catalog is redirected')
        with BACKUP_CATALOG.open('rb') as stream:
            before = _signature(os.fstat(stream.fileno()))
            global_found = find_catalog_matches(stream, unknown)
            after = _signature(os.fstat(stream.fileno()))
        if before != after:
            raise ValueError('pinned catalog changed during streaming')
        catalog_generation = {'bytes': before[3], 'mtime_ns': before[4],
                              'path_still_same_generation': _signature(BACKUP_CATALOG.stat()) == before}
    if queue.verified_report(item, workspace) != proof:
        raise ValueError('fixed E proof changed while estimating')
    result = {'contract': CONTRACT, 'observed_at_utc': datetime.now(UTC).isoformat(),
        'source_name': args.name, 'dataset': entry['dataset'], 'D_snapshot_id': base['D_snapshot_id'],
        'D_manifest_sha256': fixed.manifest_sha256, 'D_inventory_sha256': base['D_inventory_sha256'],
        'chunk_bytes': chunk_bytes, 'unchanged_file_count': unchanged,
        'changed_large_files': len(changed), 'changed_large_logical_bytes': total_bytes,
        'base_reused_extent_bytes': reused_bytes,
        'not_in_base_unique_chunks': len(unknown), 'not_in_base_unique_chunk_bytes': sum(unknown.values()),
        'known_in_older_global_catalog_unique_chunks': len(global_found) if args.global_catalog else None,
        'known_in_older_global_catalog_unique_bytes': sum(global_found.values()) if args.global_catalog else None,
        'not_seen_in_base_or_old_catalog_unique_bytes': (sum(unknown.values()) - sum(global_found.values()))
            if args.global_catalog else None,
        'changed_small_files': len(small), 'changed_small_logical_bytes': sum(row[2] for row in small),
        'small_pack_physical_bytes_estimated': False, 'catalog_generation': catalog_generation,
        'E_changed_large_files_full_sha_checked': True, 'current_source_freshness_checked': False,
        'D_object_contents_verified': False, 'D_complete_census_claimed': False,
        'NAS_acceptance_claimed': False, 'backup_or_retirement_authorized': False,
        'source_files_deleted': 0, 'elapsed_seconds': time.monotonic() - started}
    atomic_write_json(output, result, durable=True)
    progress('completed_planning_not_acceptance')
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
