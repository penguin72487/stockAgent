"""Read an E-only dedup candidate without publishing or retiring its source.

The path index is disposable scratch. Original paths with equal SHA and size
may reference the same ZIP member; extraction always creates a distinct file
with its own mode. This is not yet the canonical D/NAS snapshot ABI.
"""
from __future__ import annotations

from contextlib import closing
import errno
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import uuid
import zipfile

from stockagent.data_sync import dedup_stream_candidate as candidate, packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, rename_without_overwrite

FICLONE = 0x40049409


def _key(path: str) -> bytes:
    return path.encode('utf-8', errors='surrogatepass')


class CandidateReader:
    """Validate a candidate's fixed metadata once and read named files on demand."""
    def __init__(self, stage: Path, dataset: str, expected_sha: str, *, scratch: Path):
        self.stage = Path(stage).absolute()
        self.dataset = p.validate_slug(dataset, 'dataset')
        self.expected = p._validate_hash(expected_sha, 'original manifest SHA')
        self.scratch = Path(scratch).absolute()
        self.temp = self.db = None
        self.result = None
        self.packages = {}

    def __enter__(self):
        try:
            self._open()
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, *_args):
        self.close()

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        if self.temp is not None:
            self.temp.cleanup()
            self.temp = None

    def _open(self):
        if (self.scratch.is_symlink() or self.scratch.resolve(strict=True) != self.scratch
                or not self.scratch.is_dir() or p._paths_overlap(self.stage, self.scratch)):
            raise SnapshotError('candidate reader scratch must be a separate real directory')
        self.result = candidate.read_sealed(self.stage, self.dataset, self.expected)
        self.packages = {p._validate_hash(item['sha256'], 'candidate pack SHA'): item
                         for item in self.result['packages']}
        if len(self.packages) != len(self.result['packages']):
            raise SnapshotError('candidate contains duplicate pack objects')
        self.temp = tempfile.TemporaryDirectory(prefix='stockagent-dedup-reader-', dir=self.scratch)
        self.db = sqlite3.connect(str(Path(self.temp.name)/'index.sqlite'))
        self.db.execute('PRAGMA journal_mode=OFF')
        self.db.execute('PRAGMA synchronous=OFF')
        self.db.execute('CREATE TABLE entries (path BLOB PRIMARY KEY, kind TEXT NOT NULL, mode INTEGER NOT NULL, '
                        'size INTEGER, sha TEXT, object_sha TEXT, member BLOB, target BLOB) WITHOUT ROWID')
        refs, content_locations, directories = {}, {}, {''}
        stats = {digest: [0, 0] for digest in self.packages}
        files = logical = 0
        inventory = self.result['inventory']
        pseudo_manifest = {'archive': {'inventory': {'relpath': inventory['name'], 'sha256': inventory['sha256'],
                                                      'bytes': inventory['bytes']}}}
        for row in p._iter_inventory(self.stage, pseudo_manifest):
            path, kind, mode = row['path'], row.get('kind'), row.get('mode')
            parent = path.rpartition('/')[0]
            if parent not in directories:
                raise SnapshotError('candidate inventory parent is not a directory')
            if type(mode) is not int or not 0 <= mode <= 0o7777:
                raise SnapshotError('candidate inventory has an invalid mode')
            size = digest = obj = member = target = None
            if kind == 'file':
                size, digest, storage = row.get('size'), p._validate_hash(row.get('sha256'), 'file SHA'), row.get('storage')
                if (type(size) is not int or size < 0 or not isinstance(storage, dict)
                        or storage.get('kind') != 'pack'):
                    raise SnapshotError('candidate inventory file is invalid')
                obj = p._validate_hash(storage.get('object_sha256'), 'candidate pack SHA')
                raw_member = storage.get('member')
                if not isinstance(raw_member, str) or p._safe_relative_path(raw_member, 'candidate member').as_posix() != raw_member:
                    raise SnapshotError('candidate inventory member path is invalid')
                if obj not in self.packages:
                    raise SnapshotError('candidate inventory references an unknown pack')
                member = _key(raw_member)
                identity = (obj, member)
                existing = refs.setdefault(identity, (digest, size))
                if existing != (digest, size):
                    raise SnapshotError('candidate alias points to conflicting content')
                if content_locations.setdefault(digest, (obj, member, size)) != (obj, member, size):
                    raise SnapshotError('candidate content has more than one physical representative')
                stats[obj][0] += 1
                stats[obj][1] += size
                files += 1
                logical += size
            elif kind == 'symlink':
                value = row.get('target')
                if not isinstance(value, str):
                    raise SnapshotError('candidate inventory symlink is invalid')
                p._validate_symlink_target(path, value)
                target = _key(value)
            elif kind != 'directory':
                raise SnapshotError('candidate inventory kind is invalid')
            if kind == 'directory':
                directories.add(path)
            self.db.execute('INSERT INTO entries VALUES (?,?,?,?,?,?,?,?)',
                            (_key(path), kind, mode, size, digest, obj, member, target))
        if files != self.result['files'] or logical != self.result['logical_bytes']:
            raise SnapshotError('candidate inventory logical totals differ')
        for obj, item in self.packages.items():
            wanted = {member.decode('utf-8', errors='surrogatepass'): pair
                      for (digest, member), pair in refs.items() if digest == obj}
            if (stats[obj] != [item['file_count'], item['logical_bytes']]
                    or len(wanted) != item['stored_file_count']
                    or sum(pair[1] for pair in wanted.values()) != item['stored_logical_bytes']):
                raise SnapshotError('candidate pack logical or physical counts differ')
            path = s._safe_stage_file(self.stage, item['name'])
            with zipfile.ZipFile(path) as archive:
                if archive.namelist() != sorted(wanted):
                    raise SnapshotError('candidate ZIP members differ from its inventory')
                for name, (_digest, size) in wanted.items():
                    if archive.getinfo(name).file_size != size:
                        raise SnapshotError('candidate ZIP member size differs')
                    row = self.db.execute('SELECT kind,size,sha,object_sha,member FROM entries WHERE path=?',
                                          (_key(name),)).fetchone()
                    if row != ('file', size, _digest, obj, _key(name)):
                        raise SnapshotError('candidate representative is not its own fixed inventory path')
            if not s._signature_matches(path, item['E_signature']):
                raise SnapshotError('candidate pack generation changed during indexing')
        if (len(refs) != self.result['unique_contents'] or len(content_locations) != len(refs)
                or sum(value[2] for value in content_locations.values()) !=
                   self.result['unique_content_logical_bytes']):
            raise SnapshotError('candidate unique content count differs')
        self.db.commit()

    def copy_file(self, relative: str, destination: Path) -> dict:
        """Materialize one exact file, checking its SHA; never overwrite."""
        if self.db is None:
            raise SnapshotError('candidate reader is not open')
        if p._safe_relative_path(relative, 'candidate requested path').as_posix() != relative:
            raise SnapshotError('candidate requested path is invalid')
        row = self.db.execute('SELECT kind,mode,size,sha,object_sha,member FROM entries WHERE path=?',
                              (_key(relative),)).fetchone()
        if row is None or row[0] != 'file':
            raise SnapshotError('candidate requested path is not a regular file')
        _kind, mode, size, digest, obj, member = row
        destination = Path(destination).absolute()
        parent = destination.parent
        if (parent.is_symlink() or parent.resolve(strict=True) != parent or not parent.is_dir()
                or os.path.lexists(destination) or p._paths_overlap(self.stage, destination)):
            raise SnapshotError('candidate destination exists, overlaps its source, or is redirected')
        source = s._safe_stage_file(self.stage, self.packages[obj]['name'])
        if not s._signature_matches(source, self.packages[obj]['E_signature']):
            raise SnapshotError('candidate pack generation changed before reading')
        temporary = parent/('.dedup-read-'+uuid.uuid4().hex+'.partial')
        try:
            with zipfile.ZipFile(source) as archive, archive.open(member.decode('utf-8', errors='surrogatepass')) as stream:
                p._copy_member_and_verify(stream, temporary, expected_size=size, expected_sha256=digest,
                                          mode=mode, durable=True)
            if not s._signature_matches(source, self.packages[obj]['E_signature']):
                raise SnapshotError('candidate pack generation changed while reading')
            rename_without_overwrite(temporary, destination)
            p._fsync_directory(parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return {'path': relative, 'bytes': size, 'sha256': digest, 'mode': mode,
                'independent_file': True, 'D_published': False, 'source_retirement_authorized': False}

    @staticmethod
    def _copy_independent(source: Path, destination: Path, *, expected_size: int, mode: int, durable: bool,
                          durability_queue=None):
        """Use filesystem CoW when available, otherwise copy via the kernel.

        Hardlinks are never used: two equal original files must remain
        independently writable, including when their source modes differ.
        """
        src = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        dst = None
        try:
            before = os.fstat(src)
            if not stat.S_ISREG(before.st_mode) or before.st_size != expected_size:
                raise SnapshotError('candidate decoded representative changed')
            dst = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            cloned = True
            try:
                fcntl.ioctl(dst, FICLONE, src)
            except OSError as error:
                if error.errno not in {errno.EXDEV, errno.EINVAL, errno.ENOTTY, errno.EOPNOTSUPP, errno.ENOSYS}:
                    raise
                cloned = False
                remaining = expected_size
                while remaining:
                    try:
                        if not hasattr(os, 'copy_file_range'):
                            raise OSError(errno.ENOSYS, 'copy_file_range is unavailable')
                        count = os.copy_file_range(src, dst, min(remaining, 8*1024**2))
                    except OSError as copy_error:
                        if copy_error.errno not in {errno.EXDEV, errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                            raise
                        block = os.read(src, min(remaining, 8*1024**2))
                        if not block:
                            raise SnapshotError('candidate decoded representative ended early')
                        view = memoryview(block)
                        count = len(view)
                        while view:
                            written = os.write(dst, view)
                            if written <= 0:
                                raise OSError('short candidate restore write')
                            view = view[written:]
                    if count <= 0 or count > remaining:
                        raise SnapshotError('candidate decoded representative ended early')
                    remaining -= count
            after, copied = os.fstat(src), os.fstat(dst)
            if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                    (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    or copied.st_size != expected_size):
                raise SnapshotError('candidate decoded representative changed while copying')
            os.fchmod(dst, mode)
            if durable:
                if durability_queue is None:
                    os.fsync(dst)
                else:
                    durability_queue.submit(dst)
            return cloned
        finally:
            os.close(src)
            if dst is not None:
                os.close(dst)

    def restore_all(self, destination: Path, *, durable=False, progress=None, guard=lambda: None) -> dict:
        """Restore all logical paths once, with one decode per unique member.

        The target is invisible until a no-replace rename succeeds. It is a
        fresh materialization, never a cold publication or retirement proof.
        Any interrupted partial stays in the private scratch directory.
        """
        if self.db is None or type(durable) is not bool:
            raise SnapshotError('candidate reader or durability option is invalid')
        destination = Path(destination).absolute()
        parent = destination.parent
        if (parent.is_symlink() or parent.resolve(strict=True) != parent or not parent.is_dir()
                or os.path.lexists(destination) or p._paths_overlap(self.stage, destination)):
            raise SnapshotError('candidate restore destination exists, overlaps its source, or is redirected')
        stage = Path(tempfile.mkdtemp(prefix='.dedup-restore-', dir=parent))
        self.db.execute("CREATE INDEX IF NOT EXISTS pack_members ON entries(object_sha,member,path) WHERE kind='file'")
        count = logical = cloned = copied = 0
        try:
            for raw, _mode in self.db.execute("SELECT path,mode FROM entries WHERE kind='directory' ORDER BY path"):
                relative = raw.decode('utf-8', errors='surrogatepass')
                (stage/relative).mkdir(mode=0o700)
            for obj, item in self.packages.items():
                guard()
                source = s._safe_stage_file(self.stage, item['name'])
                if not s._signature_matches(source, item['E_signature']):
                    raise SnapshotError('candidate pack generation changed before restoration')
                with zipfile.ZipFile(source) as archive:
                    for info in sorted(archive.infolist(), key=lambda value: value.header_offset):
                        guard()
                        rows = self.db.execute("SELECT path,mode,size,sha FROM entries WHERE kind='file' "
                                               'AND object_sha=? AND member=? ORDER BY path',
                                               (obj, _key(info.filename)))
                        first = rows.fetchone()
                        if first is None:
                            raise SnapshotError('candidate stored member has no logical path')
                        raw, mode, size, digest = first
                        first_path = stage/raw.decode('utf-8', errors='surrogatepass')
                        with archive.open(info) as stream:
                            p._copy_member_and_verify(stream, first_path, expected_size=size,
                                                      expected_sha256=digest, mode=mode, durable=durable)
                        count += 1
                        logical += size
                        original_mode = mode
                        # This root is private and unpublished. A representative
                        # with mode 000 still needs to serve as the copy source;
                        # restore its exact mode before publishing the tree.
                        if not original_mode & stat.S_IRUSR:
                            os.chmod(first_path, original_mode | stat.S_IRUSR, follow_symlinks=False)
                        try:
                            for raw, mode, current_size, current_sha in rows:
                                if current_size != size or current_sha != digest:
                                    raise SnapshotError('candidate alias changed during restoration')
                                target = stage/raw.decode('utf-8', errors='surrogatepass')
                                if self._copy_independent(first_path, target, expected_size=size, mode=mode, durable=durable):
                                    cloned += 1
                                else:
                                    copied += 1
                                count += 1
                                logical += size
                                if count % 8192 == 0:
                                    guard()
                                    if progress is not None:
                                        progress({'files_completed': count, 'files_total': self.result['files'],
                                                  'logical_bytes_completed': logical,
                                                  'logical_bytes_total': self.result['logical_bytes']})
                        finally:
                            if not original_mode & stat.S_IRUSR:
                                fd = os.open(first_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                                try:
                                    os.fchmod(fd, original_mode)
                                    if durable:
                                        os.fsync(fd)
                                finally:
                                    os.close(fd)
                if not s._signature_matches(source, item['E_signature']):
                    raise SnapshotError('candidate pack generation changed while restoring')
            if count != self.result['files'] or logical != self.result['logical_bytes']:
                raise SnapshotError('candidate restored file counts differ')
            for raw, target in self.db.execute("SELECT path,target FROM entries WHERE kind='symlink' ORDER BY path"):
                (stage/raw.decode('utf-8', errors='surrogatepass')).symlink_to(
                    target.decode('utf-8', errors='surrogatepass'))
            for raw, mode in self.db.execute("SELECT path,mode FROM entries WHERE kind='directory' ORDER BY path DESC"):
                os.chmod(stage/raw.decode('utf-8', errors='surrogatepass'), mode, follow_symlinks=False)
            guard()
            rename_without_overwrite(stage, destination)
            p._fsync_directory(parent)
            return {'state': 'candidate_materialized', 'files': count, 'logical_bytes': logical,
                    'reflinked_aliases': cloned, 'copied_aliases': copied, 'durable_files': durable,
                    'independent_files': True, 'D_published': False, 'source_retirement_authorized': False}
        except BaseException as error:
            raise SnapshotError(f'candidate restoration interrupted; private partial retained: {stage}') from error
