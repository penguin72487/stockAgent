"""Build a source-local E archive from an exact retained handoff index.

This supports C generations that differ from the latest D release. The result
is an intermediate fixed source archive; it is not a published D snapshot.
"""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import stat
import time
import uuid

from stockagent.data_sync import handoff_source_repack as h, packed_snapshots as p
from stockagent.data_sync import stream_repack as s, workspace_relocation as relocation
from stockagent.data_sync.workspace_source_handoff import _same_source_generation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_metadata import MetadataRows


def prepare(source: Path, stage: Path, dataset: str, identity: str, index: Path, *,
            original_inode: int, original_lock_inode: int, preserved_paths: list[str],
            supplemental_root_lock=False, source_overrides=None, guard=lambda: None, progress=None,
            preserve_internal_symlinks=False, large_member_deflate_workers=1):
    source, stage, index = (Path(path).absolute() for path in (source, stage, index))
    dataset = p.validate_slug(dataset, 'dataset')
    identity = p._validate_hash(identity, 'fixed handoff index identity')
    if type(preserve_internal_symlinks) is not bool:
        raise SnapshotError('index archive symlink policy differs')
    if type(large_member_deflate_workers) is not int or not 1 <= large_member_deflate_workers <= 4:
        raise SnapshotError('index archive deflate worker count differs')
    if p._paths_overlap(source, stage) or stage.is_symlink():
        raise SnapshotError('index archive stage overlaps its original source')
    if (not isinstance(preserved_paths, list) or '.download.lock' not in preserved_paths
            or len(set(preserved_paths)) != len(preserved_paths)):
        raise SnapshotError('index archive protected locks differ')
    for name in preserved_paths:
        if not isinstance(name, str) or p._safe_relative_path(name, 'protected lock').as_posix() != name:
            raise SnapshotError('index archive protected lock path differs')
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise SnapshotError('index archive stage is redirected or not private')
    intent = {'contract': s.CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': identity}
    path = stage/'intent.json'
    if path.exists():
        if path.is_symlink() or json.loads(path.read_text()) != intent:
            raise SnapshotError('index archive stage belongs to another transaction')
    else:
        if any(stage.iterdir()):
            raise SnapshotError('index archive stage is not empty')
        atomic_write_json(path, intent, durable=True)
    guard()
    index_before = relocation.signature(index)
    source_rebind = h._source_index_rebind(source, index, original_inode)
    if (stage/'prepared.json').exists():
        result = s._read_prepared(stage, dataset, identity)
        if (result.get('source_mode') != 'retained_handoff_C_index'
                or result.get('source_root_inode') != original_inode
                or not s._generation_matches(index, result.get('source_index_signature'), index_before)
                or result.get('preserved_paths') != preserved_paths
                or result.get('supplemental_root_lock') is not supplemental_root_lock
                or type(result.get('large_member_deflate_workers', 1)) is not int
                or not 1 <= result.get('large_member_deflate_workers', 1) <= 4
                or result.get('preserve_internal_symlinks', False) is not preserve_internal_symlinks
                or result.get('source_generation_overrides', {}) != (source_overrides or {})):
            raise SnapshotError('completed index archive source identity differs')
        return result
    with closing(h._index_connection(index)) as db, \
            MetadataRows(p._entry_record, p._entry_from_record, memory_rows=p.METADATA_MEMORY_ROWS) as output:
        overrides = h._validated_source_overrides(dataset, db, source_overrides)
        last_scan_report = 0.0
        def scan_report(phase):
            def update(completed, total):
                nonlocal last_scan_report
                guard()
                now = time.monotonic()
                if progress is not None and (completed == 0 or completed == total or now-last_scan_report >= 1):
                    last_scan_report = now
                    progress({'stage': phase, 'inventory_items_completed': completed,
                              'inventory_items_total': total})
            return update
        h._check_source(source, db, original_inode=original_inode, original_lock_inode=original_lock_inode,
                        supplemental_root_lock=supplemental_root_lock, source_rebind=source_rebind,
                        source_overrides=overrides, progress=scan_report('checking_source_namespace'),
                        preserve_internal_symlinks=preserve_internal_symlinks)
        checkpoint = s._Checkpoint(stage, dataset, identity)
        try:
            s._reset_unfinished(stage, (item['name'] for item in checkpoint.packages))
            query = 'SELECT path,kind,sig,sha,bytes,state FROM files ORDER BY path'
            total_entries = db.execute('SELECT count(*) FROM files').fetchone()[0]
            index_progress = scan_report('indexing_source_inventory')
            index_progress(0, total_entries)
            for processed, (name, kind, signature, digest, size, state) in enumerate(db.execute(query), 1):
                if p._safe_relative_path(name, 'indexed source member').as_posix() != name:
                    raise SnapshotError('indexed source path differs')
                signature = json.loads(signature)
                if (kind not in ({'directory', 'file', 'symlink'} if preserve_internal_symlinks else {'directory', 'file'})
                        or not isinstance(signature, list) or len(signature) != 9):
                    raise SnapshotError('indexed source member metadata differs')
                if kind == 'file':
                    p._validate_hash(digest, 'fixed source member SHA')
                    if state != 'verified' or type(size) is not int or size != signature[3]:
                        raise SnapshotError('indexed source file was not completely verified')
                target = None
                if kind == 'symlink':
                    if state != 'verified' or size != 0:
                        raise SnapshotError('indexed source symlink was not verified')
                    target = h._checked_internal_symlink(source, db, name, digest)
                item = p._SourceEntry(path=name, kind=kind, mode=stat.S_IMODE(signature[2]),
                    size=size if kind == 'file' else 0, sha256=digest if kind == 'file' else None,
                    target=target)
                completed = checkpoint.lookup(name) if kind == 'file' else None
                if completed is not None:
                    item.storage = {'kind': 'pack', 'member': name, 'object_sha256': completed}
                output.append(item)
                if processed % 1024 == 0 or processed == total_entries:
                    index_progress(processed, total_entries)
            def report(event):
                guard()
                if progress is not None:
                    progress(event)
            builder = s._PackBuilder(stage, output, checkpoint=checkpoint, progress=report,
                                     large_member_deflate_workers=large_member_deflate_workers)
            try:
                for name, kind, signature, digest, size, state in db.execute(query):
                    if kind != 'file' or checkpoint.lookup(name) is not None:
                        continue
                    guard()
                    target = source/name
                    if target.parent.resolve(strict=True) != target.parent:
                        raise SnapshotError('indexed source member parent redirected')
                    actual, expected = relocation.signature(target), overrides.get(name, json.loads(signature))
                    if ((name == '.download.lock' and actual[1] != original_lock_inode)
                            or (name != '.download.lock' and not _same_source_generation(actual, expected, source_rebind))):
                        raise SnapshotError('indexed source generation changed before packing')
                    item = p._SourceEntry(path=name, kind='file', mode=stat.S_IMODE(expected[2]),
                                          size=size, sha256=digest, source_stat=p._source_stat(target.lstat()))
                    with p._open_source_entry(source, item) as stream:
                        builder.consume(stream, p._entry_record(item))
                packages = builder.close()
            except BaseException:
                builder.abort()
                raise
            files, logical = db.execute("SELECT count(*),coalesce(sum(bytes),0) FROM files WHERE kind='file'").fetchone()
            if builder.files != files or builder.bytes != logical:
                raise SnapshotError('index archive did not consume all fixed source files')
            h._check_source(source, db, original_inode=original_inode, original_lock_inode=original_lock_inode,
                            supplemental_root_lock=supplemental_root_lock, source_rebind=source_rebind,
                            source_overrides=overrides, progress=scan_report('rechecking_source_namespace'),
                            preserve_internal_symlinks=preserve_internal_symlinks)
            if relocation.signature(index) != index_before:
                raise SnapshotError('fixed handoff index changed during packing')
            temporary = stage/('inventory-'+uuid.uuid4().hex+'.partial')
            inventory_progress = scan_report('writing_E_inventory')
            inventory_progress(0, total_entries)
            digest = p._write_inventory(temporary, output,
                progress=lambda completed: inventory_progress(completed, total_entries))
            inventory = stage/('inventory-'+digest+'.jsonl.gz')
            os.rename(temporary, inventory)
            p._fsync_directory(stage)
        finally:
            checkpoint.close()
        checkpoint_path = s._safe_stage_file(stage, 'checkpoint.sqlite')
        body = {'contract': s.CONTRACT+'.prepared', 'dataset': dataset,
            'old_manifest_sha256': identity, 'old_snapshot_id': 'handoff-index-'+identity[:24],
            'packages': packages, 'inventory': {'sha256': digest, 'name': inventory.name,
                'bytes': inventory.stat().st_size, 'E_signature': s._signature(inventory)},
            'resume_index': {'name': checkpoint_path.name, 'bytes': checkpoint_path.stat().st_size,
                             'E_signature': s._signature(checkpoint_path)},
            'files': files, 'logical_bytes': logical, 'source_member_sha_streamed': True,
            'destination_independent_readback': False, 'raw_extraction_tree_created': False,
            'source_mode': 'retained_handoff_C_index', 'source_root_inode': original_inode,
            'source_index_signature': index_before, 'preserved_paths': preserved_paths,
            'supplemental_root_lock': supplemental_root_lock, 'observed_at_epoch': time.time()}
        if overrides:
            body['source_generation_overrides'] = overrides
        if preserve_internal_symlinks:
            body['preserve_internal_symlinks'] = True
        if large_member_deflate_workers != 1:
            body['large_member_deflate_workers'] = large_member_deflate_workers
        body['identity_sha256'] = s._identity(body)
        atomic_write_json(stage/'prepared.json', body, durable=True)
        return body
