"""Stream selected exact original members once for source-generation merges.

This is not full release verification or independent recovery acceptance. It
checks each selected original's length/SHA while it is consumed, and fences
the fixed manifest, inventory, selected object generations and owner inode.
"""
from __future__ import annotations

import hashlib
import os
import json
from pathlib import Path
import stat
import zipfile

from stockagent.data_sync import packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError


def _signature(path: Path):
    if path.is_symlink() or path.resolve(strict=True) != path:
        raise SnapshotError('streamed packed input is redirected')
    item = path.lstat()
    if not stat.S_ISREG(item.st_mode):
        raise SnapshotError('streamed packed input is not regular')
    return (item.st_dev, item.st_ino, item.st_mode, item.st_size,
            item.st_mtime_ns, item.st_ctime_ns, item.st_nlink)


def read_stable_bytes(path: Path, *, maximum: int) -> bytes:
    """Read a bounded regular source under one stable, non-symlink inode."""
    if type(maximum) is not int or maximum <= 0:
        raise SnapshotError('source read requires a positive byte limit')
    before = _signature(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        opened = os.fstat(handle.fileno())
        opened_sig = (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_size,
                      opened.st_mtime_ns, opened.st_ctime_ns, opened.st_nlink)
        if opened_sig != before or opened.st_size > maximum:
            raise SnapshotError('source member changed or exceeds its byte limit')
        data = handle.read(maximum + 1)
        closed = os.fstat(handle.fileno())
        closed_sig = (closed.st_dev, closed.st_ino, closed.st_mode, closed.st_size,
                      closed.st_mtime_ns, closed.st_ctime_ns, closed.st_nlink)
    if len(data) != before[3] or closed_sig != before or _signature(path) != before:
        raise SnapshotError('source member changed while reading')
    return data


def stream_members(root: Path, resolved, *, names, consume, maximum_file_bytes: int,
                   maximum_total_bytes: int) -> dict:
    """Call ``consume(path, original_bytes)`` in physical pack order.

    The callback must not publish or delete data before this function returns:
    a later member or immutable-object generation check may still fail.
    """
    if (type(maximum_file_bytes) is not int or not 0 < maximum_file_bytes <= 256*1024**2
            or type(maximum_total_bytes) is not int or not 0 < maximum_total_bytes <= 2*1024**4
            or isinstance(names, (str, bytes)) or not callable(consume)):
        raise SnapshotError('member streaming requires explicit bounded admission')
    selected_names = set()
    for name in names:
        if (not isinstance(name, str) or p._safe_relative_path(name, 'streamed member').as_posix() != name
                or name in selected_names or len(selected_names) >= 4096):
            raise SnapshotError('member streaming names are invalid or duplicated')
        selected_names.add(name)
    if not selected_names:
        raise SnapshotError('member streaming requires selected names')
    root = Path(root).absolute()
    p._check_packed_read_authority(root)
    root = root.resolve(strict=True)
    owner = root/'.local-state/locks/publish-retention-global.lock'
    if owner.is_symlink() or not owner.is_file():
        raise SnapshotError('streaming requires the existing publication owner')
    with p._try_publication_lock(owner, shared=True):
        manifest_path = resolved.manifest_path.absolute()
        expected = p._path_under(root, f'manifests/{resolved.manifest["dataset"]}/'
                                 f'{resolved.manifest["snapshot_id"]}.json', 'manifest')
        if manifest_path != expected:
            raise SnapshotError('streamed manifest path differs from its identity')
        manifest_sig = _signature(manifest_path)
        manifest_bytes = read_stable_bytes(manifest_path, maximum=16*1024**2)
        if (hashlib.sha256(manifest_bytes).hexdigest() != resolved.manifest_sha256
                or json.loads(manifest_bytes) != resolved.manifest or _signature(manifest_path) != manifest_sig):
            raise SnapshotError('streamed manifest changed or has a different identity')
        p._validate_manifest(resolved.manifest)
        inventory = p._read_object_path(root, resolved.manifest['archive']['inventory']['relpath'], 'inventory')
        inventory_sig = _signature(inventory)
        with p._open_inventory(root, resolved.manifest) as entries:
            p._validate_inventory(resolved.manifest, entries, retain_object_members=False)
            selected = [row for row in entries if row['path'] in selected_names]
        if (len(selected) != len(selected_names) or any(row.get('kind') != 'file' for row in selected)
                or any(type(row.get('size')) is not int or row['size'] > maximum_file_bytes for row in selected)
                or sum(row['size'] for row in selected) > maximum_total_bytes):
            raise SnapshotError('streamed selection is missing or exceeds the admitted size')
        objects = {str(item['sha256']): item for item in resolved.manifest['archive']['objects']}
        required = {sha for row in selected for sha, _ in p._storage_extents(row['storage'], row['size'])}
        object_signatures = {}
        for sha in required:
            item = objects.get(sha)
            if item is None:
                raise SnapshotError('streamed selected object is absent from the manifest')
            path = p._read_object_path(root, item['relpath'], 'selected object')
            signature = _signature(path)
            if signature[3] != item['bytes']:
                raise SnapshotError('streamed selected object length differs')
            object_signatures[path] = signature
        seen, count = set(), 0

        def one(stream, row):
            nonlocal count
            name = row['path']
            if name in seen or name not in selected_names:
                raise SnapshotError('streamed original was returned more than once')
            chunks = p._verified_member_blocks(stream, row['size'], row['sha256'], label=name)
            value = b''.join(chunks)
            consume(name, value)
            seen.add(name)
            count += len(value)

        try:
            p._materialize_indexed_files(root, resolved.manifest, selected, None,
                d_primary_native_blob_reads=False, d_primary_native_pack_reads=False,
                durable=False, on_copy=None, durability_queue=None, consume_file=one)
        except (zipfile.BadZipFile, EOFError) as error:
            raise SnapshotError('selected packed source cannot be decoded') from error
        if (seen != selected_names or count != sum(row['size'] for row in selected)
                or _signature(manifest_path) != manifest_sig or _signature(inventory) != inventory_sig
                or any(_signature(path) != signature for path, signature in object_signatures.items())):
            raise SnapshotError('streamed source identity changed before its complete result')
        p._check_packed_read_authority(root)
    return {'files': len(seen), 'logical_bytes': count,
            'original_member_sha256_verified': True, 'whole_encoded_object_sha256_verified': False,
            'independent_recovery_verified': False}
