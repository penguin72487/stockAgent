"""Disposable sealed-pack routing hints owned by the original migration journal.

These are not manifests, source observations, or recovery receipts. A retry must
hash every current original, and canonical all-file recovery must still pass.
Only a closed, installed pack is eligible; partial ZIPs are never resumed.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat

from stockagent.data_sync.desync_snapshots import (
    SnapshotError, _canonical_json_bytes, _path_under, _safe_relative_path,
    validate_slug, write_immutable_json,
)

CONTRACT = 'sealed_pack_routing_hints_v1'
MAX_HINT_BYTES = 32 * 1024**2
_NAME = re.compile(r'[0-9a-f]{64}\.json\Z')


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class SealedPackHints:
    def __init__(self, root: Path, cold: Path, source: Path, dataset: str):
        self.root = root.absolute()
        self.binding = {'contract': CONTRACT, 'cold': str(cold), 'source': str(source),
                        'dataset': validate_slug(dataset, 'checkpoint dataset')}
        if (self.root.resolve() != self.root or self.root.is_relative_to(cold)
                or self.root.is_relative_to(source)):
            raise SnapshotError('sealed-pack hints must remain in their real private journal')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise SnapshotError('sealed-pack hint owner/permissions differ')
        self._root_identity = (info.st_dev, info.st_ino)
        self.receipts: dict[str, dict] = {}
        self.statistics = {'hint_files': 0, 'files_loaded': 0, 'objects_loaded': 0,
                           'missing_objects': 0, 'rejected': 0}

    def _recheck_root(self):
        info = self.root.lstat()
        if (not stat.S_ISDIR(info.st_mode) or self.root.resolve() != self.root
                or (info.st_dev, info.st_ino) != self._root_identity
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise SnapshotError('sealed-pack hint authority changed')

    def _read(self, path):
        self._recheck_root()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                    or before.st_size > MAX_HINT_BYTES):
                raise SnapshotError('sealed-pack hint is not bounded owned metadata')
            raw = stream.read(MAX_HINT_BYTES + 1)
            after = os.fstat(stream.fileno())
        if (_signature(before) != _signature(after)
                or _signature(after) != _signature(path.lstat())
                or len(raw) != before.st_size
                or hashlib.sha256(raw).hexdigest() != path.stem):
            raise SnapshotError('sealed-pack hint signature/hash differs')
        self._recheck_root()
        return json.loads(raw)

    def _validate(self, document):
        from stockagent.data_sync import packed_snapshots as packed
        if not isinstance(document, dict) or document.get('binding') != self.binding:
            raise SnapshotError('sealed-pack hint source/authority binding differs')
        item, rows = document.get('object'), document.get('files')
        if not isinstance(item, dict) or not isinstance(rows, list) or not 1 <= len(rows) <= packed.MAX_PACK_MEMBERS:
            raise SnapshotError('sealed-pack hint is not one bounded pack')
        digest = packed._validate_hash(item.get('sha256'), 'sealed pack')
        if (set(item) != {'bucket', 'bytes', 'file_count', 'kind', 'logical_bytes', 'relpath', 'sha256'}
                or item.get('kind') != 'pack'
                or item.get('relpath') != packed._object_relpath('packs', digest, '.zip').as_posix()
                or any(type(item.get(k)) is not int or item[k] < 0 for k in
                       ('bucket', 'bytes', 'file_count', 'logical_bytes'))
                or not 0 <= item['bucket'] < 4096 or item['file_count'] != len(rows)):
            raise SnapshotError('sealed-pack hint descriptor differs')
        previous = ''
        for row in rows:
            if not isinstance(row, dict):
                raise SnapshotError('invalid sealed-pack file hint')
            name = row.get('path')
            if (not isinstance(name, str) or _safe_relative_path(name, 'sealed pack member').as_posix() != name
                    or name <= previous or row.get('kind') != 'file'
                    or type(row.get('size')) is not int or row['size'] < 0
                    or type(row.get('mode')) is not int
                    or row.get('storage') != {'kind': 'pack', 'member': name, 'object_sha256': digest}):
                raise SnapshotError('invalid sealed-pack member/order/storage')
            previous = name
        # Reuse the canonical size/SHA/mode/member/count validator, not a second
        # file semantics implementation. This temporary shape is NOT published.
        packed._validate_inventory({'schema_version': 1, 'source': {
            'files': len(rows), 'directories': 0, 'symlinks': 0,
            'logical_bytes': item['logical_bytes']}, 'archive': {'objects': [item]}}, rows)
        return item, rows

    def load_into(self, rows, objects):
        """Load bounded fragments; current original SHA is ALWAYS checked later.

        Invalid cache entries are preserved and counted, never accepted or used
        to block a fresh canonical publication. Missing objects simply disable
        the corresponding hint. Existing committed inventories retain priority.
        """
        self._recheck_root()
        for path in sorted(self.root.iterdir()):
            if not _NAME.fullmatch(path.name):
                if not path.name.startswith('.syncthing.'):
                    self.statistics['rejected'] += 1
                continue
            try:
                document = self._read(path)
                item, members = self._validate(document)
                self.receipts[str(path)] = {'path': str(path), 'sha256': path.stem}
                self.statistics['hint_files'] += 1
                if rows is None:
                    continue
                target = _path_under(Path(self.binding['cold']), item['relpath'], 'sealed pack')
                try:
                    observed = target.lstat()
                except FileNotFoundError:
                    self.statistics['missing_objects'] += 1
                    continue
                if not stat.S_ISREG(observed.st_mode) or observed.st_size != item['bytes']:
                    self.statistics['rejected'] += 1
                    continue
                for member in members:
                    rows.upsert(member)
                objects[item['sha256']] = item
                self.statistics['files_loaded'] += len(members)
                self.statistics['objects_loaded'] += 1
            except (OSError, SnapshotError, ValueError, TypeError, KeyError, RecursionError):
                self.statistics['rejected'] += 1
        self._recheck_root()

    def record(self, item, entries):
        self._recheck_root()
        document = {'binding': self.binding, 'object': item,
                    'files': sorted((entry.inventory_row() for entry in entries), key=lambda row: row['path'])}
        self._validate(document)
        raw = _canonical_json_bytes(document)
        if len(raw) > MAX_HINT_BYTES:
            self.statistics['rejected'] += 1
            return
        path = self.root / (hashlib.sha256(raw).hexdigest() + '.json')
        if path.exists() or path.is_symlink():
            try:
                self._read(path)
            except (OSError, SnapshotError, ValueError, RecursionError):
                self.statistics['rejected'] += 1
                return  # Preserve bad cache evidence; never overwrite it.
        else:
            write_immutable_json(path, document)
        if str(path) not in self.receipts:
            self.statistics['hint_files'] += 1
        self.receipts[str(path)] = {'path': str(path), 'sha256': path.stem}
        self._recheck_root()
