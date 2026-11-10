"""Versioned content-alias inventory rules for packed snapshots.

This module is a reader contract under development. Importing it neither
publishes a head nor grants permission to retire an existing version. A v3
release stores one ZIP member for equal SHA-256/length content and keeps each
original path, mode and symlink in its ordered logical inventory.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import time
import zipfile
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable, Mapping

from stockagent.data_sync.desync_snapshots import SnapshotError

SCHEMA_VERSION = 3
ARCHIVE_FORMAT = 'stockagent-content-alias-zip-v3'
MAX_UNIQUE_CONTENTS = 500_000
STRICT_POLICY = 'unique-content-v1'
INCREMENTAL_POLICY = 'incremental-shared-v1'


def validate_inventory(manifest: Mapping[str, Any], entries: Iterable[dict], *,
                       retain_object_members: bool = True, progress=None) -> dict:
    """Validate logical paths and unique physical ZIP-member mappings.

    Reuse all v2 inventory/path/selection checks on a streaming projection.
    The projection is never persisted and cannot turn an alias into v2 data.
    Additional checks bind every alias to one same-content representative and
    distinguish logical reference counts from physically stored members.
    """
    from stockagent.data_sync import packed_snapshots as p

    if (manifest.get('schema_version') != SCHEMA_VERSION
            or manifest.get('archive', {}).get('format') != ARCHIVE_FORMAT
            or type(retain_object_members) is not bool):
        raise SnapshotError('content-alias inventory requires its explicit v3 contract')
    policy = manifest['archive'].get('alias_policy', STRICT_POLICY)
    if policy not in {STRICT_POLICY, INCREMENTAL_POLICY}:
        raise SnapshotError('content-alias policy is unknown')
    objects = manifest['archive'].get('objects')
    if not isinstance(objects, list):
        raise SnapshotError('content-alias objects must be a list')
    by_object = {}
    for item in objects:
        if (not isinstance(item, Mapping) or item.get('kind') not in {'pack', 'blob'}
                or policy == STRICT_POLICY and item.get('kind') != 'pack'):
            raise SnapshotError('v3 content-alias object kind is invalid')
        digest = p._validate_hash(item.get('sha256'), 'content-alias object SHA')
        if digest in by_object:
            raise SnapshotError('v3 content-alias object is repeated')
        if item['kind'] == 'pack' and (any(type(item.get(field)) is not int or item[field] < 0 for field in
                ('file_count', 'logical_bytes', 'stored_file_count', 'stored_logical_bytes'))
                or item.get('member_selection') not in ({None} if policy == STRICT_POLICY else {None, 'subset'})):
            raise SnapshotError('v3 content-alias object counts or selection are invalid')
        by_object[digest] = item

    # One bounded entry per unique member, never one per logical alias. Each
    # value is [content SHA, length, number of logical paths].
    refs: dict[tuple[str, str], list] = {}
    physical_by_content: dict[str, tuple[str, str, int]] = {}
    representatives: set[tuple[str, str]] = set()
    direct_blobs: dict[str, list[int]] = {}
    previous_path = ''

    def projection():
        nonlocal previous_path
        for row in entries:
            if not isinstance(row, dict) or not isinstance(row.get('path'), str):
                raise SnapshotError('content-alias inventory row is invalid')
            path = p._safe_relative_path(row['path'], 'content-alias inventory path').as_posix()
            if path != row['path'] or path <= previous_path:
                raise SnapshotError('content-alias inventory paths are noncanonical or unsorted')
            previous_path = path
            if row.get('kind') != 'file':
                yield row
                continue
            storage, size = row.get('storage'), row.get('size')
            if (not isinstance(storage, Mapping) or storage.get('kind') not in {'pack', 'blob', 'chunks'}
                    or type(size) is not int or size < 0):
                raise SnapshotError('content-alias file storage is invalid')
            if storage['kind'] != 'pack':
                if policy == STRICT_POLICY:
                    raise SnapshotError('strict content-alias files require ZIP packs')
                if storage['kind'] == 'blob':
                    obj = p._validate_hash(storage.get('object_sha256'), 'content-alias object SHA')
                    totals = direct_blobs.setdefault(obj, [0, 0])
                    totals[0] += 1
                    totals[1] += size
                yield row
                continue
            member = storage.get('member')
            if (not isinstance(member, str)
                    or p._safe_relative_path(member, 'content-alias ZIP member').as_posix() != member):
                raise SnapshotError('content-alias ZIP member is invalid')
            obj = p._validate_hash(storage.get('object_sha256'), 'content-alias object SHA')
            digest = p._validate_hash(row.get('sha256'), 'content-alias file SHA')
            if obj not in by_object:
                raise SnapshotError('content alias references an unknown object')
            key = (obj, member)
            current = refs.get(key)
            if current is None:
                if len(refs) >= MAX_UNIQUE_CONTENTS:
                    raise SnapshotError('content-alias unique-member bound exceeded')
                refs[key] = [digest, size, 1]
            elif current[:2] != [digest, size]:
                raise SnapshotError('content aliases disagree about one physical member')
            else:
                current[2] += 1
            identity = (obj, member, size)
            if policy == STRICT_POLICY and physical_by_content.setdefault(digest, identity) != identity:
                raise SnapshotError('one content SHA has multiple representatives or lengths')
            if member == path:
                representatives.add(key)
            # The existing v2 validator checks logical counts, modes, source
            # selection and object references. Its path==member invariant is
            # true only in this in-memory projection, never on stored bytes.
            projected_storage = {**storage, 'member': path}
            yield {**row, 'storage': projected_storage}

    projected_objects = [{key: value for key, value in item.items() if key != 'member_selection'}
                         if item['kind'] == 'pack' else dict(item) for item in objects]
    projected_manifest = {**manifest, 'schema_version': p.CHUNKED_SNAPSHOT_SCHEMA_VERSION,
                          'archive': {**manifest['archive'], 'objects': projected_objects}}
    result = p._validate_inventory(projected_manifest, projection(),
                                   retain_object_members=False, progress=progress)
    if policy == STRICT_POLICY and (set(refs) != representatives or len(physical_by_content) != len(refs)):
        raise SnapshotError('content-alias representative is missing from its logical inventory')
    per_object = {digest: [0, 0] for digest, item in by_object.items() if item['kind'] == 'pack'}
    for (obj, _member), (_digest, size, _count) in refs.items():
        per_object[obj][0] += 1
        per_object[obj][1] += size
    for obj, totals in per_object.items():
        item = by_object[obj]
        expected = [item['stored_file_count'], item['stored_logical_bytes']]
        if (totals != expected and (item.get('member_selection') != 'subset'
                or totals[0] > expected[0] or totals[1] > expected[1])):
            raise SnapshotError('content-alias stored member totals differ')
    members = {obj: [] for obj in per_object}
    if retain_object_members:
        for obj, member in refs:
            members[obj].append(member)
        for names in members.values():
            names.sort()
    result['object_members'] = members if retain_object_members else {}
    result['alias_members'] = refs if retain_object_members else {}
    result['direct_blob_stats'] = direct_blobs
    result['unique_content_files'] = len(refs)
    result['unique_content_logical_bytes'] = sum(value[1] for value in refs.values())
    return result


def verify_inventory(sync_root: Path, resolved, entries, *, materialized_path=None,
                     reconstruct_paths=None, reconstruct_all=False, native_pack_reads=False,
                     native_blob_reads=False, native_chunk_reads=False,
                     progress=None, materialized_file_workers=1, pack_verification=None) -> dict:
    """Check v3 packs and, when requested, decode each unique content once.

    This is a future reader operation, not a migration precondition. A full
    logical reconstruction proves each alias points to the checked member; it
    does not claim that old independent source occurrences were reread.
    """
    from stockagent.data_sync import packed_snapshots as p

    if pack_verification is not None:
        raise SnapshotError('v3 content-alias releases require a v3-capable remote verification contract')
    if reconstruct_all and reconstruct_paths is not None:
        raise SnapshotError('choose either all-file or explicit-path reconstruction')
    manifest = resolved.manifest
    validation = validate_inventory(manifest, entries, progress=progress)
    selected = None if reconstruct_paths is None else set(reconstruct_paths)
    selected_rows = {} if selected is None else {
        row['path']: row for row in entries if row['kind'] == 'file' and row['path'] in selected}
    if selected is not None and set(selected_rows) != selected:
        raise SnapshotError('content-alias reconstruction request contains an unknown or non-file path')
    objects = manifest['archive']['objects']
    total_bytes = sum(item['bytes'] for item in objects)
    verified_bytes = verified_objects = logical_files = logical_bytes = unique_files = 0
    for item in objects:
        path = p._read_object_path(sync_root, item['relpath'], 'content-alias object relpath')
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size != item['bytes']:
            raise SnapshotError('content-alias packed object generation or length differs')
        if item['kind'] == 'blob' and native_blob_reads:
            from stockagent.data_sync.windows_cold_io import hash_file
            observed_sha = hash_file(path)
        elif item['kind'] == 'blob' or not native_pack_reads:
            observed_sha = p.sha256_file(path)
        else:
            observed_sha = item['sha256']  # full SHA is checked by the native pack reader
        if observed_sha != item['sha256']:
            raise SnapshotError('content-alias packed object SHA differs')
        try:
            if item['kind'] == 'pack':
                expected_names = validation['object_members'][item['sha256']]
                with p._native_pack_source(path, item, enabled=native_pack_reads) as reader, \
                        zipfile.ZipFile(reader, mode='r') as archive:
                    names = archive.namelist()
                    if ((item.get('member_selection') == 'subset'
                            and not p._is_ordered_subset(expected_names, names))
                            or (item.get('member_selection') != 'subset' and names != expected_names)):
                        raise SnapshotError('content-alias ZIP members differ from the fixed inventory')
                    if reconstruct_all:
                        for name in names:
                            member = validation['alias_members'].get((item['sha256'], name))
                            with archive.open(name) as stream:
                                if member is None:
                                    while stream.read(8*1024**2):
                                        pass  # CRC-check a retained, no-longer-referenced member
                                else:
                                    digest, size, count = member
                                    for _block in p._verified_member_blocks(stream, size, digest, label=name):
                                        pass
                            if member is not None:
                                unique_files += 1
                                logical_files += count
                                logical_bytes += count*size
                    else:
                        bad = archive.testzip()
                        if bad is not None:
                            raise SnapshotError('content-alias ZIP CRC failed')
                    if selected is not None:
                        for row in selected_rows.values():
                            storage = row['storage']
                            if storage['kind'] != 'pack' or storage['object_sha256'] != item['sha256']:
                                continue
                            with archive.open(storage['member']) as stream:
                                for _block in p._verified_member_blocks(stream, row['size'], row['sha256'],
                                                                      label=row['path']):
                                    pass
                            logical_files += 1
                            logical_bytes += row['size']
            elif reconstruct_all:
                count, size = validation['direct_blob_stats'].get(item['sha256'], [0, 0])
                logical_files += count
                logical_bytes += size
            elif selected is not None:
                for row in selected_rows.values():
                    storage = row['storage']
                    if storage['kind'] == 'blob' and storage['object_sha256'] == item['sha256']:
                        logical_files += 1
                        logical_bytes += row['size']
        except (OSError, KeyError, zipfile.BadZipFile, EOFError) as error:
            raise SnapshotError('content-alias ZIP cannot be decoded') from error
        after = path.lstat()
        if (p._source_stat(before), before.st_mode, before.st_nlink) != (
                p._source_stat(after), after.st_mode, after.st_nlink):
            raise SnapshotError('content-alias packed object changed during verification')
        verified_objects += 1
        verified_bytes += item['bytes']
        if progress is not None:
            progress({'stage': 'verify-cold-objects', 'completed_items': verified_objects,
                      'total_items': len(objects), 'completed_bytes': verified_bytes, 'total_bytes': total_bytes})
    if reconstruct_all or selected is not None:
        objects_by_sha = {item['sha256']: item for item in objects}
        for row in entries:
            if (row.get('kind') != 'file' or row['storage']['kind'] != 'chunks'
                    or selected is not None and row['path'] not in selected):
                continue
            digest, size = hashlib.sha256(), 0
            for block in p._iter_chunked_file(sync_root, objects_by_sha, row,
                                              native=native_blob_reads or native_chunk_reads):
                digest.update(block)
                size += len(block)
            if size != row['size'] or digest.hexdigest() != row['sha256']:
                raise SnapshotError('content-alias chunked file SHA differs')
            logical_files += 1
            logical_bytes += size
    result = {'snapshot_id': manifest['snapshot_id'], 'manifest_sha256': resolved.manifest_sha256,
              'inventory_sha256': manifest['archive']['inventory']['sha256'], 'objects': len(objects),
              'verified_object_bytes': verified_bytes, 'materialized_verified': False,
              'chunked_files_verified': 0}
    if reconstruct_all or selected is not None:
        expected_files = manifest['source']['files'] if reconstruct_all else len(selected_rows)
        expected_bytes = manifest['source']['logical_bytes'] if reconstruct_all else sum(row['size'] for row in selected_rows.values())
        if (logical_files, logical_bytes) != (expected_files, expected_bytes):
            raise SnapshotError('content-alias reconstruction totals differ')
        result.update(independently_reconstructed_files=logical_files,
                      independently_reconstructed_bytes=logical_bytes,
                      content_alias_reconstruction={'contract': 'content_alias_unique_member_sha_v1',
                          'unique_members_decoded': unique_files if reconstruct_all else None,
                          'old_independent_occurrences_reread': False})
    if materialized_path is not None:
        p._verify_materialized(materialized_path, manifest, entries, progress=progress,
                               file_workers=materialized_file_workers)
        result['materialized_verified'] = True
    return result


def materialize(sync_root: Path, manifest: Mapping[str, Any], entries: Iterable[dict],
                destination_for, *, native_pack_reads: bool, durable: bool, native_blob_reads: bool = False,
                on_copy=None, durability_queue=None, consume_file=None) -> None:
    """Decode each physical member once, then copy aliases as independent files.

    A disposable SQLite grouping index bounds memory for millions of logical
    paths and lives in the existing admitted metadata scratch location. A
    bounded in-memory consumer may request individual paths without creating
    that index. This function never publishes a ready marker by itself.
    """
    from stockagent.data_sync import packed_snapshots as p
    from stockagent.data_sync.dedup_candidate_reader import CandidateReader
    from stockagent.data_sync.packed_metadata import _metadata_scratch_location

    if (manifest.get('schema_version') != SCHEMA_VERSION or type(native_pack_reads) is not bool
            or type(native_blob_reads) is not bool
            or type(durable) is not bool):
        raise SnapshotError('content-alias materialization options are invalid')
    objects = {str(row['sha256']): row for row in manifest['archive']['objects']}
    if consume_file is not None:
        groups = {}
        for row in entries:
            if row.get('kind') != 'file':
                continue
            storage = row['storage']
            if storage['kind'] == 'chunks':
                with closing(p._ChunkedFileReader(p._iter_chunked_file(
                        sync_root, objects, row, native=native_blob_reads))) as stream:
                    consume_file(stream, row)
                if on_copy is not None:
                    on_copy(row)
            else:
                groups.setdefault(storage['object_sha256'], []).append(row)
        for digest, rows in groups.items():
            item = objects[digest]
            path = p._read_object_path(sync_root, item['relpath'], 'content-alias object relpath')
            if item['kind'] == 'pack':
                with p._native_pack_source(path, item, enabled=native_pack_reads) as reader, \
                        zipfile.ZipFile(reader, 'r') as archive:
                    for row in rows:
                        with archive.open(row['storage']['member']) as stream:
                            consume_file(stream, row)
                        if on_copy is not None:
                            on_copy(row)
            else:
                from stockagent.data_sync.windows_cold_io import binary_reader
                for row in rows:
                    with (binary_reader(path) if native_blob_reads else path.open('rb')) as stream:
                        consume_file(stream, row)
                    if on_copy is not None:
                        on_copy(row)
        return

    if destination_for is None:
        raise SnapshotError('content-alias materialization needs an explicit destination')
    scratch, reserve, workspace_scratch = _metadata_scratch_location()
    next_capacity_check = 0.0
    with tempfile.TemporaryDirectory(prefix='stockagent-alias-index-', dir=scratch) as directory, \
            closing(sqlite3.connect(str(Path(directory)/'index.sqlite'))) as db:
        db.execute('PRAGMA journal_mode=OFF')
        db.execute('PRAGMA synchronous=OFF')
        db.execute('PRAGMA temp_store=FILE')
        db.execute('CREATE TABLE aliases (object_sha TEXT NOT NULL, member BLOB NOT NULL, '
                   'path BLOB NOT NULL, body TEXT NOT NULL, PRIMARY KEY(object_sha,member,path)) WITHOUT ROWID')
        count = copied_files = 0
        for row in entries:
            if row.get('kind') != 'file':
                continue
            if count % 8192 == 0:
                if shutil.disk_usage(scratch).free < reserve:
                    raise SnapshotError('content-alias metadata scratch is below its reserve')
                if workspace_scratch and time.monotonic() >= next_capacity_check:
                    from stockagent.data_sync.workspace_volume import admit
                    admit(0)
                    next_capacity_check = time.monotonic()+60
            storage = row['storage']
            if storage['kind'] == 'chunks':
                with closing(p._ChunkedFileReader(p._iter_chunked_file(
                        sync_root, objects, row, native=native_blob_reads))) as stream:
                    p._copy_member_and_verify(stream, destination_for(row), expected_size=int(row['size']),
                        expected_sha256=str(row['sha256']), mode=int(row['mode']), durable=durable,
                        durability_queue=durability_queue)
                count += 1
                copied_files += 1
                if on_copy is not None:
                    on_copy(row)
                continue
            member = storage['member'].encode('utf-8', errors='surrogatepass') if storage['kind'] == 'pack' else b''
            db.execute('INSERT INTO aliases VALUES (?,?,?,?)',
                (storage['object_sha256'], member,
                 row['path'].encode('utf-8', errors='surrogatepass'),
                 json.dumps(row, ensure_ascii=True, separators=(',', ':'))))
            count += 1
        db.commit()
        def emit(cursor, opener):
            nonlocal copied_files
            first = cursor.fetchone()
            if first is None:
                return False
            row = json.loads(first[0])
            first_path = destination_for(row)
            original_mode = int(row['mode'])
            with opener() as stream:
                p._copy_member_and_verify(stream, first_path, expected_size=int(row['size']),
                    expected_sha256=str(row['sha256']), mode=original_mode | stat.S_IRUSR,
                    durable=durable, durability_queue=durability_queue)
            copied_files += 1
            if on_copy is not None:
                on_copy(row)
            try:
                for (body,) in cursor:
                    alias = json.loads(body)
                    if alias['sha256'] != row['sha256'] or alias['size'] != row['size']:
                        raise SnapshotError('content-alias mapping changed during materialization')
                    CandidateReader._copy_independent(first_path, destination_for(alias),
                        expected_size=int(row['size']), mode=int(alias['mode']), durable=durable,
                        durability_queue=durability_queue)
                    copied_files += 1
                    if on_copy is not None:
                        on_copy(alias)
            finally:
                if not original_mode & stat.S_IRUSR:
                    descriptor = os.open(first_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                    try:
                        os.fchmod(descriptor, original_mode)
                        if durable:
                            if durability_queue is None:
                                os.fsync(descriptor)
                            else:
                                durability_queue.submit(descriptor)
                    finally:
                        os.close(descriptor)
            return True

        for digest, item in objects.items():
            path = p._read_object_path(sync_root, item['relpath'], 'content-alias object relpath')
            if item['kind'] == 'pack':
                with p._native_pack_source(path, item, enabled=native_pack_reads) as reader, \
                        zipfile.ZipFile(reader, 'r') as archive:
                    for info in sorted(archive.infolist(), key=lambda member: member.header_offset):
                        cursor = db.execute('SELECT body FROM aliases WHERE object_sha=? AND member=? ORDER BY path',
                                            (digest, info.filename.encode('utf-8', errors='surrogatepass')))
                        if not emit(cursor, lambda: archive.open(info, 'r')) and item.get('member_selection') != 'subset':
                            raise SnapshotError('content-alias ZIP member has no logical path')
            else:
                from stockagent.data_sync.windows_cold_io import binary_reader
                cursor = db.execute('SELECT body FROM aliases WHERE object_sha=? AND member=? ORDER BY path',
                                    (digest, b''))
                emit(cursor, lambda: binary_reader(path) if native_blob_reads else path.open('rb'))
        if copied_files != count:
            raise SnapshotError('not every content-alias path was materialized')
