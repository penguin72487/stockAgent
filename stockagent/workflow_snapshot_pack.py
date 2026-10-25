"""Private, explicit storage backend for one immutable workflow observation."""
from contextlib import contextmanager
import gzip
import hashlib
import os
from pathlib import Path
import re
import stat
import struct
import zipfile

from stockagent.data_sync.packed_snapshots import _SeekablePackBuffer, MAX_PACK_MEMBERS
from stockagent.workflow_snapshot_streams import PinnedSnapshotStreams, signature, require

CONTRACT = 'workflow_snapshot_pack_v1'
ARCHIVE = 'evidence/evidence.pack.zip'
NAME = re.compile(r'evidence/([0-9a-f]{64})\.gz')


class ReadablePackBuffer(_SeekablePackBuffer):
    """Reuse ZIP backpatch buffering; flush before a rare in-flight reread."""

    def seekable(self):
        return True

    def read(self, size=-1):
        self.flush()
        self._stream.seek(self._position)
        raw = self._stream.read(size)
        self._position = self._start = self._stream.tell()
        return raw


class SnapshotObject:
    def __init__(self, store, name, raw_bytes):
        match = NAME.fullmatch(name)
        require(match is not None, 'noncanonical packed snapshot name')
        self.store, self.name, self.digest = store, name, match[1]
        self.raw_bytes = raw_bytes

    def exists(self):
        return self.digest in self.store.members

    def write_bytes(self, compressed):
        self.store.append(self.digest, self.raw_bytes, compressed)
        return len(compressed)

    def read_bytes(self):
        return self.store.compressed(self.name)


class SnapshotPackStore:
    """Own a new pack exclusively; no source cache, publisher or cleanup path."""

    def __init__(self, output, *, max_file_bytes, buffer_bytes=8 << 20):
        require(type(max_file_bytes) is int and 0 < max_file_bytes < 1 << 32,
                'invalid packed raw member budget')
        require(type(buffer_bytes) is int and buffer_bytes > 0, 'invalid pack buffer')
        self.output, self.limit = Path(output), max_file_bytes
        self.members, self.descriptor = {}, None
        self.closed, self.failed = False, False
        self.directory = self.stream = self.archive = None
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            self.directory = os.open(self.output / 'evidence', flags)
            fd = os.open('evidence.pack.zip', os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW,
                         0o600, dir_fd=self.directory)
            try:
                self.stream = os.fdopen(fd, 'w+b', buffering=0)
            except BaseException:
                os.close(fd)
                raise
            self.buffer = ReadablePackBuffer(self.stream, capacity=buffer_bytes)
            self.archive = zipfile.ZipFile(self.buffer, 'x', compression=zipfile.ZIP_STORED, allowZip64=True)
            self.directory_signature = signature(os.fstat(self.directory))
            self.identity = os.fstat(self.stream.fileno()).st_ino
            self._directories()
        except BaseException:
            self.abort()
            raise

    def _directories(self):
        require(self.output.resolve(strict=True) == self.output, 'packed output path changed')
        require(signature(os.fstat(self.directory)) == self.directory_signature
                == signature(os.stat(self.output / 'evidence', follow_symlinks=False)),
                'packed evidence directory generation changed')

    def destination(self, name, raw_bytes):
        require(type(raw_bytes) is int and 0 <= raw_bytes <= self.limit, 'invalid packed raw size')
        return SnapshotObject(self, name, raw_bytes)

    def append(self, digest, raw_bytes, compressed):
        require(not self.closed and not self.failed, 'packed snapshot writer unavailable')
        require(digest not in self.members and len(self.members) < MAX_PACK_MEMBERS,
                'packed snapshot member admission exceeded')
        require(type(raw_bytes) is int and 0 <= raw_bytes <= self.limit, 'invalid packed raw size')
        require(isinstance(compressed, bytes) and 18 <= len(compressed) <= raw_bytes * 2 + 1024,
                'invalid packed gzip size')
        require(compressed[:3] == b'\x1f\x8b\x08'
                and struct.unpack('<I', compressed[-4:])[0] == raw_bytes,
                'packed gzip raw size differs')
        info = zipfile.ZipInfo(digest + '.gz', date_time=(1980, 1, 1, 0, 0, 0))
        info.create_system, info.external_attr = 3, (stat.S_IFREG | 0o644) << 16
        info.compress_type = zipfile.ZIP_STORED
        try:
            self.archive.writestr(info, compressed)
        except BaseException:
            self.failed = True
            raise
        self.members[digest] = dict(name=info.filename, raw_bytes=raw_bytes, gzip_bytes=len(compressed))

    def compressed(self, name):
        match = NAME.fullmatch(name)
        require(match is not None and match[1] in self.members, 'unknown packed snapshot')
        if self.closed:
            require(self.descriptor is not None, 'unsealed packed snapshot')
            with SnapshotStreams(self.output, lambda key: self.output / key, storage=self.descriptor) as streams:
                return streams.compressed(name)
        require(not self.failed, 'packed snapshot writer failed')
        self.buffer.flush()
        before = signature(os.fstat(self.stream.fileno()))
        raw = self.archive.read(self.members[match[1]]['name'])
        require(signature(os.fstat(self.stream.fileno())) == before
                == signature(os.stat('evidence.pack.zip', dir_fd=self.directory, follow_symlinks=False)),
                'live packed snapshot generation changed')
        return raw

    def seal(self, sources):
        if self.descriptor is not None:
            return self.descriptor
        require(not self.closed and not self.failed, 'packed snapshot writer failed before seal')
        expected = {}
        for row in sources:
            if row['state'] != 'captured':
                continue
            match = NAME.fullmatch(row['snapshot'])
            require(match is not None and match[1] == row['sha256'], 'packed source identity differs')
            require(row['sha256'] not in expected or expected[row['sha256']] == row['bytes'],
                    'duplicate packed source size differs')
            expected[row['sha256']] = row['bytes']
        require(expected.keys() == self.members.keys()
                and all(self.members[key]['raw_bytes'] == value for key, value in expected.items()),
                'packed source member inventory differs')
        self._directories()
        try:
            self.archive.close()
            self.buffer.flush()
            self._directories()
            before = os.fstat(self.stream.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_ino == self.identity,
                    'packed output identity differs')
            require(signature(before) == signature(os.stat('evidence.pack.zip', dir_fd=self.directory, follow_symlinks=False)),
                    'packed output replaced')
            digest = self.buffer.complete_sha256()
            if digest is None:
                self.stream.seek(0)
                digest = hashlib.file_digest(self.stream, 'sha256').hexdigest()
            require(signature(before) == signature(os.fstat(self.stream.fileno()))
                    == signature(os.stat('evidence.pack.zip', dir_fd=self.directory, follow_symlinks=False)),
                    'packed output changed while sealing')
            self._directories()
            self.descriptor = dict(contract=CONTRACT, archive=ARCHIVE, archive_bytes=before.st_size,
                archive_sha256=digest, maximum_raw_member_bytes=self.limit,
                members=dict(self.members), member_count=len(self.members), complete=True,
                durability_scope='closed payload; independent reconstruction required; no cold publication')
        finally:
            self._close_descriptors()
        return self.descriptor

    def _close_descriptors(self):
        try:
            if self.stream is not None:
                self.stream.close()
        finally:
            try:
                if self.directory is not None:
                    os.close(self.directory)
                    self.directory = None
            finally:
                self.closed = True

    def abort(self):
        if self.closed:
            return
        self.failed = True
        try:
            if self.archive is not None:
                self.archive.close()
            if hasattr(self, 'buffer'):
                self.buffer.flush()
        except BaseException:
            pass
        finally:
            self._close_descriptors()


class BoundedDecoded:
    def __init__(self, stream, size):
        self.stream, self.expected, self.consumed, self.eof = stream, size, 0, False

    def read(self, size=-1):
        available = self.expected - self.consumed + 1
        requested = available if size is None or size < 0 else min(size, available)
        raw = self.stream.read(requested)
        self.consumed += len(raw)
        require(self.consumed <= self.expected, 'packed gzip exceeds admitted raw size')
        if requested and len(raw) < requested:
            self.eof = True
        return raw


class SnapshotStreams:
    """One pinned immutable pack or the unchanged legacy snapshot opener."""

    def __init__(self, output, local, *, storage=None):
        self.output, self.local, self.storage = Path(output), local, storage
        self.pin = PinnedSnapshotStreams(output, local)
        self.handle = self.archive = None

    def __enter__(self):
        self.pin.__enter__()
        if self.storage is None:
            return self
        try:
            value = self.storage
            require(value.get('contract') == CONTRACT and value.get('complete') is True
                    and value.get('archive') == ARCHIVE, 'unknown snapshot storage contract')
            members = value.get('members')
            require(isinstance(members, dict) and type(value.get('member_count')) is int
                    and value['member_count'] == len(members) <= MAX_PACK_MEMBERS, 'invalid pack member count')
            limit = value.get('maximum_raw_member_bytes')
            require(type(limit) is int and 0 < limit < 1 << 32, 'invalid packed raw member budget')
            for digest, item in members.items():
                require(re.fullmatch(r'[0-9a-f]{64}', digest) is not None
                        and item['name'] == digest + '.gz'
                        and type(item['raw_bytes']) is int and 0 <= item['raw_bytes'] <= limit
                        and type(item['gzip_bytes']) is int and 18 <= item['gzip_bytes'] <= item['raw_bytes'] * 2 + 1024,
                        'invalid packed member identity or size')
            require(self.pin.evidence_fd is not None, 'packed evidence directory is not canonical')
            flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0)
            descriptor = os.open('evidence.pack.zip', flags, dir_fd=self.pin.evidence_fd)
            try:
                self.handle = os.fdopen(descriptor, 'rb', buffering=1 << 20)
            except BaseException:
                os.close(descriptor)
                raise
            self.before = os.fstat(self.handle.fileno())
            require(stat.S_ISREG(self.before.st_mode), 'packed snapshot is not regular')
            digest = hashlib.file_digest(self.handle, 'sha256').hexdigest()
            require(self.before.st_size == value['archive_bytes'] and digest == value['archive_sha256'],
                    'packed archive SHA or size differs')
            self.handle.seek(0)
            self.archive = zipfile.ZipFile(self.handle)
            entries = self.archive.infolist()
            require(len(entries) == len(members) and {item.filename for item in entries}
                    == {item['name'] for item in members.values()}, 'packed member inventory differs')
            for item in members.values():
                info = self.archive.getinfo(item['name'])
                require(info.compress_type == zipfile.ZIP_STORED and not info.flag_bits & 1
                        and stat.S_ISREG(info.external_attr >> 16)
                        and info.file_size == item['gzip_bytes'], 'packed member encoding differs')
            self._generation()
        except BaseException:
            self.__exit__(*__import__('sys').exc_info())
            raise
        return self

    def _generation(self):
        require(signature(self.before) == signature(os.fstat(self.handle.fileno()))
                == signature(os.stat('evidence.pack.zip', dir_fd=self.pin.evidence_fd, follow_symlinks=False)),
                'packed snapshot generation changed')

    def _item(self, name):
        match = NAME.fullmatch(name)
        require(match is not None and match[1] in self.storage['members'], 'unknown packed snapshot')
        return self.storage['members'][match[1]]

    def compressed(self, name):
        require(self.archive is not None, 'compressed pack read requires packed layout')
        return self.archive.read(self._item(name)['name'])

    @contextmanager
    def open(self, name):
        if self.storage is None:
            with self.pin.open(name) as stream:
                yield stream
            return
        item = self._item(name)
        with self.archive.open(item['name']) as compressed, gzip.GzipFile(fileobj=compressed) as decoded:
            bounded = BoundedDecoded(decoded, item['raw_bytes'])
            yield bounded
            require(bounded.eof and bounded.consumed == bounded.expected,
                    'packed snapshot was not fully read')

    def __exit__(self, kind, value, traceback):
        try:
            if kind is None and self.handle is not None:
                self._generation()
        finally:
            if self.archive is not None:
                self.archive.close()
            if self.handle is not None:
                self.handle.close()
            self.pin.__exit__(kind, value, traceback)
        return False


def snapshot_json(streams, name):
    import json
    with streams.open(name) as stream:
        return json.loads(stream.read())


def validate_manifest_storage(manifest):
    storage = manifest.get('snapshot_storage')
    if storage is None:
        return
    expected = {}
    for row in manifest['files']:
        if row['state'] != 'captured':
            continue
        match = NAME.fullmatch(row['snapshot'])
        require(match is not None and match[1] == row['sha256'], 'packed source identity differs')
        require(row['sha256'] not in expected or expected[row['sha256']] == row['bytes'],
                'duplicate packed source size differs')
        expected[row['sha256']] = row['bytes']
    members = storage['members']
    require(expected.keys() == members.keys()
            and all(members[key]['raw_bytes'] == size for key, size in expected.items()),
            'packed manifest source inventory differs')
