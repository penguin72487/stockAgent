"""Bounded binary pipes to native Windows FileStream, not WSL 9p data I/O.

Only the enrolled D cold namespace is allowed. Canonical authority, locking,
manifests and inode checks remain in Linux; this is solely a byte I/O adapter.
Non-D paths retain normal file I/O (including unit-test temporary directories).
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path, PureWindowsPath
import stat
import subprocess
import tempfile

from stockagent.data_sync.desync_snapshots import SnapshotError

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts/windows_cold_binary_io.ps1'
ALIASES = (
    (Path('/srv/stockagent-packed'), 'stockagent-cold-primary/packed'),
    (Path('/srv/stockagent-d-volume/stockagent-cold-primary'), 'stockagent-cold-primary'),
    (Path('/mnt/d/stockagent-cold-primary'), 'stockagent-cold-primary'),
)


def metadata_path(path):
    """Small control/index I/O uses the enrolled stable 8 KiB mount namespace.

    Native Windows binary pipes bypass 9p, but JSON can itself be hundreds of
    MiB. Do not accidentally send it through the known-failing 64 KiB mount.
    """
    path = Path(path).absolute()
    prefix = Path('/mnt/d/stockagent-cold-primary')
    try:
        relative = path.relative_to(prefix)
    except ValueError:
        return path
    if any(part in {'.','..'} for part in relative.parts) or path.resolve()!=path:
        raise SnapshotError('Redirected D metadata namespace')
    alias = Path('/srv/stockagent-d-volume/stockagent-cold-primary') / relative
    if alias.resolve()!=alias:
        raise SnapshotError('Redirected stable D metadata namespace')
    first, second = path, alias
    while not first.exists():
        first, second = first.parent, second.parent
    if not second.exists() or first.stat().st_ino!=second.stat().st_ino:
        raise SnapshotError('Stable D metadata inode differs')
    return alias


def windows_path(path):
    path = Path(path).absolute()
    for root, prefix in ALIASES:
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        if not relative.parts or any(part in {'', '.', '..'} for part in relative.parts):
            raise SnapshotError('Unsafe native D binary I/O path')
        if path.parent.resolve() != path.parent or path.is_symlink():
            raise SnapshotError('Redirected native D binary I/O path')
        return str(PureWindowsPath('D:/') / prefix / relative.as_posix())
    return None


def _process(path, *, mode, offset=0, prefix_sha256=None, reserve_bytes=32 * 1024**3):
    native = windows_path(path)
    if native is None:
        raise SnapshotError('Windows binary I/O requires fixed D cold path')
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    config = {'path': native, 'mode': mode, 'offset': offset,
              'prefix_sha256': prefix_sha256, 'reserve_bytes': reserve_bytes}
    encoded_config = base64.b64encode(json.dumps(config).encode()).decode()
    # No shell interpolation, external script path or inherited plaintext key.
    command = "$env:STOCKAGENT_BINARY_IO_CONFIG='" + encoded_config + "';\n" + SCRIPT.read_text()
    encoded = base64.b64encode(command.encode('utf-16le')).decode()
    errors = tempfile.TemporaryFile()
    process = subprocess.Popen(['powershell.exe', '-NoLogo', '-NoProfile', '-NonInteractive',
                                '-EncodedCommand', encoded], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=errors)
    return process, errors


@contextmanager
def binary_reader(path):
    path = Path(path)
    if windows_path(path) is None:
        with path.open('rb') as stream:
            yield stream
        return
    process, errors = _process(path, mode='read')
    process.stdin.close()
    try:
        yield process.stdout
        if process.wait(timeout=120):
            errors.seek(0)
            raise SnapshotError('Native Windows cold read failed: ' + errors.read(4096).decode(errors='replace'))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)
        process.stdout.close()
        errors.close()


def hash_file(path):
    before = Path(path).lstat()
    if not stat.S_ISREG(before.st_mode):
        raise SnapshotError('Native full SHA requires a regular non-redirected file')
    value = hashlib.sha256()
    size = 0
    with binary_reader(path) as stream:
        while block := stream.read(4 * 1024 * 1024):
            value.update(block)
            size += len(block)
    after = Path(path).lstat()
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_mode', 'st_nlink')
    if size != before.st_size or any(getattr(before, key) != getattr(after, key) for key in fields):
        raise SnapshotError('Cold bytes changed during native full SHA')
    return value.hexdigest()


class BinaryWriter:
    def __init__(self, path, *, offset=0, prefix_sha256=None, reserve_bytes=32 * 1024**3):
        if offset < 0 or (offset and (not isinstance(prefix_sha256, str) or len(prefix_sha256) != 64)):
            raise SnapshotError('Native append requires exact retained prefix size and SHA')
        self.path, self.offset = Path(path), offset
        self.process, self.errors = _process(path, mode='write', offset=offset,
                                            prefix_sha256=prefix_sha256, reserve_bytes=reserve_bytes)
        self.appended = 0
        self.result = None
        ready = self.process.stdout.readline(4096)
        try:
            record = json.loads(ready)
            if record.get('state') != 'native_writer_ready' or record.get('offset') != offset:
                raise SnapshotError('Native writer readiness differs; no input sent')
        except (ValueError, SnapshotError) as error:
            if self.process.poll() is None:
                self.process.terminate()
            self.process.wait(timeout=30)
            self.errors.seek(0)
            message = self.errors.read(4096).decode(errors='replace')
            self.process.stdin.close()
            self.process.stdout.close()
            self.errors.close()
            raise SnapshotError('Native Windows writer not ready: ' + message) from error

    def write(self, block):
        try:
            self.process.stdin.write(block)
        except BrokenPipeError as error:
            self.process.wait(timeout=30)
            self.errors.seek(0)
            raise SnapshotError('Native Windows writer pipe failed: ' + self.errors.read(4096).decode(errors='replace')) from error
        self.appended += len(block)

    def finish(self):
        if self.result is not None:
            return self.result
        try:
            self.process.stdin.close()
        except BrokenPipeError:
            pass
        output = self.process.stdout.read(8192)
        code = self.process.wait(timeout=120)
        if code:
            self.errors.seek(0)
            raise SnapshotError('Native Windows cold write failed: ' + self.errors.read(4096).decode(errors='replace'))
        result = json.loads(output)
        if result.get('flushed') is not True or result.get('bytes') != self.offset + self.appended:
            raise SnapshotError('Native Windows durable byte count differs')
        if self.path.lstat().st_size != result['bytes']:
            raise SnapshotError('Canonical D namespace byte count differs from native writer')
        self.result = result
        return result

    def close(self):
        try:
            if self.process.poll() is None:
                try:
                    self.finish()
                except BaseException:
                    self.process.terminate()
                    self.process.wait(timeout=30)
        finally:
            self.process.stdout.close()
            self.errors.close()
