"""Experimental streaming gzip opener; canonical reader remains the oracle."""
from contextlib import contextmanager
import errno
import gzip
import os
from pathlib import Path
import re
import stat


def signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def require(ok, message):
    if not ok:
        raise ValueError(message)


class PinnedSnapshotStreams:
    """Pin regular snapshot files, fall back to the original safe local opener.

    No content cache: each selected gzip is streamed and fully verified by the
    unchanged caller. Noncanonical names and internal symlinks retain the
    original path behavior. Generation fences reject concurrent replacement.
    """

    def __init__(self, output, local):
        self.output, self.local = Path(output), local
        self.output_fd = self.evidence_fd = None

    def __enter__(self):
        if not all(hasattr(os, name) for name in ('O_DIRECTORY', 'O_NOFOLLOW', 'O_CLOEXEC')):
            return self
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            self.output_fd = os.open(self.output, flags)
            self.evidence_fd = os.open('evidence', flags, dir_fd=self.output_fd)
            self.output_signature = signature(os.fstat(self.output_fd))
            self.evidence_signature = signature(os.fstat(self.evidence_fd))
            self.verify_directories()
        except OSError as exc:
            self.close()
            if exc.errno not in (errno.ELOOP, errno.ENOENT, errno.ENOTDIR):
                raise
        except BaseException:
            self.close()
            raise
        return self

    def close(self):
        for attribute in ('evidence_fd', 'output_fd'):
            descriptor = getattr(self, attribute)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, attribute, None)

    def verify_directories(self):
        require(self.output.resolve(strict=True) == self.output,
                'snapshot output path changed')
        require(signature(os.fstat(self.output_fd)) == self.output_signature
                == signature(os.stat(self.output, follow_symlinks=False)),
                'snapshot output directory generation changed')
        require(signature(os.fstat(self.evidence_fd)) == self.evidence_signature
                == signature(os.stat('evidence', dir_fd=self.output_fd, follow_symlinks=False)),
                'snapshot evidence directory generation changed')

    @contextmanager
    def open(self, name):
        if self.evidence_fd is None or not re.fullmatch(r'evidence/[0-9a-f]{64}\.gz', name):
            with gzip.open(self.local(name), 'rb') as stream:
                yield stream
            return
        basename = name[len('evidence/'):]
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | getattr(os, 'O_NONBLOCK', 0)
        try:
            descriptor = os.open(basename, flags, dir_fd=self.evidence_fd)
        except OSError as exc:
            if exc.errno != errno.ELOOP:
                raise
            # The canonical reader admits symlinks which stay inside output.
            with gzip.open(self.local(name), 'rb') as stream:
                yield stream
            return
        try:
            handle = os.fdopen(descriptor, 'rb')
        except BaseException:
            os.close(descriptor)
            raise
        with handle:
            before = os.fstat(handle.fileno())
            require(stat.S_ISREG(before.st_mode), 'snapshot is not a regular file')
            with gzip.GzipFile(fileobj=handle, mode='rb') as stream:
                yield stream
            require(signature(before) == signature(os.fstat(handle.fileno()))
                    == signature(os.stat(basename, dir_fd=self.evidence_fd, follow_symlinks=False)),
                    'snapshot file generation changed')

    def __exit__(self, kind, value, traceback):
        try:
            if kind is None and self.evidence_fd is not None:
                self.verify_directories()
        finally:
            self.close()
        return False
