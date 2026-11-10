"""Bounded Linux event fence for a fixed, indexed source namespace.

The caller still verifies the complete namespace and source generations and
holds its original writer locks. Events close the check-then-exchange race for
uncoordinated processes; an overflow, missing watch or unknown event defers.
"""
from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import sqlite3
import stat
import struct
import time

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.workspace_relocation import signature

_OPEN = 0x20
_MODIFY = 0x2
_ATTRIB = 0x4
_CLOSE_WRITE = 0x8
_MOVED_FROM = 0x40
_MOVED_TO = 0x80
_CREATE = 0x100
_DELETE = 0x200
_DELETE_SELF = 0x400
_MOVE_SELF = 0x800
_Q_OVERFLOW = 0x4000
_IGNORED = 0x8000
_ISDIR = 0x40000000
_ONLYDIR = 0x01000000
_DONT_FOLLOW = 0x02000000
_EVENT = struct.Struct('iIII')
_WATCH = (_OPEN | _MODIFY | _ATTRIB | _CLOSE_WRITE | _MOVED_FROM | _MOVED_TO |
          _CREATE | _DELETE | _DELETE_SELF | _MOVE_SELF | _ONLYDIR | _DONT_FOLLOW)
MAX_DIRECTORIES = 20000
MAX_REVIEWED_DIRECTORIES = 200000


class SourceMutationFence:
    """Watch only directories named by one unchanged fixed copy index."""
    def __init__(self, source: Path, destination: Path, index: Path, *, guard=lambda: None,
                 max_directories=MAX_DIRECTORIES, progress=None):
        if type(max_directories) is not int or not 1 <= max_directories <= MAX_REVIEWED_DIRECTORIES:
            raise SnapshotError('mutation fence directory limit is invalid')
        self.roots = (Path(source), Path(destination))
        self.index = Path(index)
        self.guard = guard
        self.progress = progress
        self.max_directories = max_directories
        self.fd = -1
        self.watches = {}
        self.next_poll = 0.0
        self.lock_attr_consumed = False
        self.destination_lock_signature = None

    def __enter__(self):
        libc = ctypes.CDLL(None, use_errno=True)
        libc.inotify_init1.argtypes = (ctypes.c_int,)
        libc.inotify_init1.restype = ctypes.c_int
        libc.inotify_add_watch.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32)
        libc.inotify_add_watch.restype = ctypes.c_int
        self._add = libc.inotify_add_watch
        self.fd = libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), 'cannot create source mutation fence')
        try:
            index_before = signature(self.index)
            if self.index.is_symlink() or not stat.S_ISREG(index_before[2]):
                raise SnapshotError('mutation fence index is not a regular file')
            self.destination_lock_signature = signature(self.roots[1]/'.download.lock')
            if not stat.S_ISREG(self.destination_lock_signature[2]):
                raise SnapshotError('mutation fence destination lock is not a regular file')
            for root in self.roots:
                self._watch(root, root=True)
            with sqlite3.connect(f'file:{self.index}?mode=ro', uri=True) as db:
                count = 0
                total = None
                if self.progress is not None:
                    total = db.execute("SELECT count(*) FROM files WHERE kind='directory'").fetchone()[0]
                    if total > self.max_directories:
                        raise SnapshotError('mutation fence directory limit exceeded')
                    self.progress(0, total)
                for (name,) in db.execute("SELECT path FROM files WHERE kind='directory' ORDER BY path"):
                    relative = Path(name)
                    if (not isinstance(name, str) or not name or relative.is_absolute()
                            or '..' in relative.parts or relative.as_posix() != name):
                        raise SnapshotError('mutation fence directory scope differs')
                    count += 1
                    if count > self.max_directories:
                        raise SnapshotError('mutation fence directory limit exceeded')
                    for root in self.roots:
                        self._watch(root/relative, root=False)
                    if count % 64 == 0:
                        self.guard()
                        self.check()
                        if self.progress is not None:
                            self.progress(count, total)
                if self.progress is not None:
                    if count != total:
                        raise SnapshotError('mutation fence directory count changed')
                    self.progress(count, total)
            if signature(self.index) != index_before:
                raise SnapshotError('mutation fence fixed index changed while installing watches')
            self.check()
            return self
        except BaseException:
            self.close()
            raise

    def _watch(self, path: Path, *, root: bool):
        before = signature(path)
        if (not stat.S_ISDIR(before[2]) or path.is_symlink()
                or path.resolve(strict=True) != path):
            raise SnapshotError('mutation fence directory is missing or redirected')
        wd = self._add(self.fd, os.fsencode(path), _WATCH)
        if wd < 0:
            raise OSError(ctypes.get_errno(), 'cannot watch a fixed source directory')
        if wd in self.watches or signature(path) != before:
            raise SnapshotError('mutation fence directory identity changed or is aliased')
        self.watches[wd] = (path, root)

    def check(self, *, allow_lock_attrib=False, allow_root_move=False):
        """Drain every queued event; only expected own root/lock changes pass."""
        while True:
            try:
                data = os.read(self.fd, 256*1024)
            except BlockingIOError:
                try:
                    unchanged = signature(self.roots[1]/'.download.lock') == self.destination_lock_signature
                except OSError:
                    unchanged = False
                if not unchanged:
                    raise BlockingIOError(errno.EBUSY, 'destination coordination lock changed during handoff')
                return
            if not data:
                raise SnapshotError('mutation fence unexpectedly closed')
            offset = 0
            while offset < len(data):
                if len(data)-offset < _EVENT.size:
                    raise SnapshotError('mutation fence event is truncated')
                wd, mask, _cookie, length = _EVENT.unpack_from(data, offset)
                offset += _EVENT.size
                if length > len(data)-offset:
                    raise SnapshotError('mutation fence event name is truncated')
                name = os.fsdecode(data[offset:offset+length].split(b'\0',1)[0])
                offset += length
                if mask & _Q_OVERFLOW or wd not in self.watches or mask & _IGNORED:
                    raise BlockingIOError(errno.EBUSY, 'source mutation fence lost event coverage')
                path, is_root = self.watches[wd]
                relevant = mask & ~_ISDIR
                if relevant == _OPEN and mask & _ISDIR:
                    continue  # our bounded directory census opens directories
                if is_root and name == '.download.lock' and relevant == _OPEN:
                    continue  # the owner reopens this locked, empty coordination file
                if (allow_lock_attrib and is_root and path == self.roots[0] and name == '.download.lock'
                        and relevant == _ATTRIB and not self.lock_attr_consumed):
                    self.lock_attr_consumed = True
                    continue  # the original lock was hardlinked into the placeholder
                if allow_root_move and is_root and path == self.roots[0] and not name and relevant == _MOVE_SELF:
                    continue  # the owner's atomic exchange renamed this watched root
                raise BlockingIOError(errno.EBUSY, f'source or E namespace changed during handoff '
                                      f'(name={name!r}, mask=0x{relevant:x})')

    def periodic(self):
        now = time.monotonic()
        if now >= self.next_poll:
            self.check()
            self.next_poll = now + 0.25

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __exit__(self, *_exc):
        self.close()

