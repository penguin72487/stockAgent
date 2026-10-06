"""Bounded binary pipes to native Windows FileStream, not WSL 9p data I/O.

Only the enrolled D cold namespace and guarded backup staging are allowed. Canonical authority, locking,
manifests and inode checks remain in Linux; this is solely a byte I/O adapter.
Non-D paths retain normal file I/O (including unit-test temporary directories).
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
import json
import re
from pathlib import Path, PureWindowsPath
import stat
import subprocess
import tempfile
import time

from stockagent.data_sync.desync_snapshots import SnapshotError

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts/windows_cold_binary_io.ps1'
ALIASES = (
    (Path('/srv/stockagent-packed'), 'stockagent-cold-primary/packed'),
    (Path('/srv/stockagent-d-volume/stockagent-cold-primary'), 'stockagent-cold-primary'),
    (Path('/mnt/d/stockagent-cold-primary'), 'stockagent-cold-primary'),
    (Path('/srv/stockagent-backup-ingress-lab203'), 'stockagent-backup-ingress-lab203'),
    (Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203'), 'stockagent-backup-ingress-lab203'),
)


def retryable_interop_startup(stderr: bytes) -> bool:
    """Only retry the observed WSL bridge startup timeout, before any bytes."""
    return b'WSL' in stderr and b'UtilAcceptVsock' in stderr and b'failed 110' in stderr


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


def _configuration(path, *, mode, offset=0, prefix_sha256=None, reserve_bytes=32 * 1024**3,
                   source=None, expected_sha256=None, expected_bytes=None):
    native = windows_path(path)
    if native is None:
        raise SnapshotError('Windows binary I/O requires fixed D cold path')
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    if native.startswith('D:\\stockagent-backup-ingress-lab203\\'):
        from stockagent.data_sync.packed_backup import mounted_volume
        physical = Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203')
        if (mounted_volume(Path('/srv/stockagent-d-volume'))[2] != 'D:'
                or not physical.samefile(Path('/srv/stockagent-backup-ingress-lab203'))):
            raise SnapshotError('Native transport differs from its enrolled D backing volume')
    config = {'path': native, 'mode': mode, 'offset': offset,
              'prefix_sha256': prefix_sha256, 'reserve_bytes': reserve_bytes}
    if mode == 'write' and not native.startswith('D:\\stockagent-cold-primary\\'):
        raise SnapshotError('Native stream writes remain within cold incoming storage')
    if mode == 'copy':
        origin = windows_path(source)
        if (origin is None or not origin.startswith('D:\\stockagent-cold-primary\\')
                or not native.startswith('D:\\stockagent-backup-ingress-lab203\\.staging\\')
                or type(expected_bytes) is not int or not 0 <= expected_bytes <= 8 * 1024**3
                or not isinstance(expected_sha256, str) or not re.fullmatch('[0-9a-f]{64}', expected_sha256)):
            raise SnapshotError('Native copy must preserve an exact cold source into fresh transport staging')
        config.update(source=origin, expected_sha256=expected_sha256, expected_bytes=expected_bytes)
    if mode == 'hash':
        if type(expected_bytes) is not int or not 0 <= expected_bytes <= 8 * 1024**3:
            raise SnapshotError('Native full hash exceeds its exact member bound')
        config['expected_bytes'] = expected_bytes
    return config


def _launch(configs, *, batched=False):
    # No shell interpolation, external script path or inherited plaintext key.
    if batched:
        # Metadata goes through stdin so Windows' command-line length does not
        # grow with the batch. The only executable bytes are the fixed script.
        command = "[Console]::InputEncoding = New-Object Text.UTF8Encoding($false)\n"
        command += "$batch = [Console]::In.ReadLine() | ConvertFrom-Json\nforeach ($item in $batch) {\n"
        command += "$env:STOCKAGENT_BINARY_IO_CONFIG = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($item | ConvertTo-Json -Compress)))\n"
        command += SCRIPT.read_text() + '\n}\n'
    else:
        encoded_config = base64.b64encode(json.dumps(configs[0]).encode()).decode()
        command = "$env:STOCKAGENT_BINARY_IO_CONFIG='" + encoded_config + "';\n" + SCRIPT.read_text()
    encoded = base64.b64encode(command.encode('utf-16le')).decode()
    errors = tempfile.TemporaryFile()
    process = subprocess.Popen(['powershell.exe', '-NoLogo', '-NoProfile', '-NonInteractive',
                                '-EncodedCommand', encoded], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=errors)
    return process, errors


def _process(path, **kwargs):
    return _launch([_configuration(path, **kwargs)])


def _batch(configs):
    if (not 1 <= len(configs) <= 16 or any(c['mode'] not in {'copy', 'hash'} for c in configs)
            or sum(c['expected_bytes'] for c in configs) > 16 * 1024**3):
        raise SnapshotError('Native batch exceeds its immutable I/O bounds')
    read_only = all(c['mode'] == 'hash' for c in configs)
    for attempt in range(3):
        process, errors = _launch(configs, batched=True)
        retry = False
        try:
            output, _ = process.communicate(json.dumps(configs, ensure_ascii=True).encode() + b'\n', timeout=3600)
            if process.returncode or len(output) > 16 * 1024:
                errors.seek(0)
                failure = errors.read(4096)
                if (read_only and process.returncode and not output and attempt < 2
                        and retryable_interop_startup(failure)):
                    retry = True
                else:
                    category = ('interop-startup-timeout' if retryable_interop_startup(failure)
                                else 'native-command-rejected')
                    raise SnapshotError(f'Native batch failed ({category}, exit={process.returncode}, '
                                        f'output_bytes={len(output)}); retain every partial member')
            if not retry:
                rows = [json.loads(line) for line in output.splitlines()]
                if len(rows) != len(configs):
                    raise SnapshotError('Native batch omitted an exact member proof')
                return rows
        finally:
            if process.poll() is None:
                process.terminate();process.wait(timeout=30)
            process.stdout.close();errors.close()
        if retry:
            time.sleep((0.2, 1.0)[attempt])


def _stable(before, after):
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_mode', 'st_nlink')
    return all(getattr(before, key) == getattr(after, key) for key in fields)


def copy_many(members):
    """One native process, full per-member copy/hash/flush and source proof."""
    members = [(Path(s), Path(d), sha, count) for s, d, sha, count in members]
    if not 1 <= len(members) <= 16 or len({d for _, d, _, _ in members}) != len(members):
        raise SnapshotError('Native copy batch destinations are repeated or unbounded')
    configs, signatures = [], []
    for source, destination, sha, count in members:
        before = source.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size != count:
            raise SnapshotError('Native batch source differs from its exact member')
        destination.parent.mkdir(parents=True, exist_ok=True)
        configs.append(_configuration(destination, mode='copy', source=source,
            expected_sha256=sha, expected_bytes=count, reserve_bytes=64 * 1024**3))
        signatures.append(before)
    for (source, destination, sha, count), before, proof in zip(members, signatures, _batch(configs), strict=True):
        after = destination.lstat()
        if (not stat.S_ISREG(after.st_mode) or after.st_size != count or not _stable(before, source.lstat())
                or proof.get('flushed') is not True or proof.get('bytes') != count or proof.get('sha256') != sha):
            raise SnapshotError('Native batch changed its source or exact copied bytes')
        destination.chmod(0o600)


def hash_many(paths):
    paths = [Path(p) for p in paths]
    if not 1 <= len(paths) <= 16 or len(set(paths)) != len(paths):
        raise SnapshotError('Native hash batch members are repeated or unbounded')
    signatures = [p.lstat() for p in paths]
    if any(not stat.S_ISREG(s.st_mode) for s in signatures):
        raise SnapshotError('Native batch hash needs regular exact members')
    configs = [_configuration(p, mode='hash', expected_bytes=s.st_size) for p, s in zip(paths, signatures, strict=True)]
    result = {}
    for path, before, proof in zip(paths, signatures, _batch(configs), strict=True):
        if (not _stable(before, path.lstat()) or proof.get('bytes') != before.st_size
                or not isinstance(proof.get('sha256'), str) or not re.fullmatch('[0-9a-f]{64}', proof['sha256'])):
            raise SnapshotError('Native batch full hash changed an exact member')
        result[path] = proof['sha256']
    return result


def copy_verified(source, destination, *, expected_sha256, expected_bytes):
    """Native D-to-D copy/hash/flush; Linux retains all authority/signature gates."""
    source, destination = Path(source), Path(destination)
    before = source.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes:
        raise SnapshotError('Native copy source is not the exact regular member')
    destination.parent.mkdir(parents=True, exist_ok=True)
    process, errors = _process(destination, mode='copy', source=source,
        expected_sha256=expected_sha256, expected_bytes=expected_bytes, reserve_bytes=64 * 1024**3)
    process.stdin.close()
    try:
        output = process.stdout.read(8192)
        if process.wait(timeout=3600):
            raise SnapshotError('Native immutable transport copy failed; preserve its partial bytes')
        result = json.loads(output)
        after = source.lstat()
        fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_mode', 'st_nlink')
        if (result.get('flushed') is not True or result.get('bytes') != expected_bytes
                or result.get('sha256') != expected_sha256 or destination.lstat().st_size != expected_bytes
                or any(getattr(before, key) != getattr(after, key) for key in fields)):
            raise SnapshotError('Native immutable copy changed its source or exact bytes')
        destination.chmod(0o600)
    finally:
        if process.poll() is None:
            process.terminate();process.wait(timeout=30)
        process.stdout.close();errors.close()


@contextmanager
def binary_reader(path):
    path = Path(path)
    if windows_path(path) is None:
        with path.open('rb') as stream:
            yield stream
        return
    for attempt in range(3):
        process, errors = _process(path, mode='read')
        process.stdin.close()
        retry = False
        try:
            # A failed interop launch used to appear as a zero-byte checksum
            # failure after creating a restore member. Peek preserves every
            # actual byte, and retry is limited to the known empty startup.
            if not process.stdout.peek(1) and process.wait(timeout=30):
                errors.seek(0)
                failure = errors.read(4096)
                if attempt < 2 and retryable_interop_startup(failure):
                    retry = True
                else:
                    raise SnapshotError('Native Windows cold read failed: ' + failure.decode(errors='replace'))
            if not retry:
                yield process.stdout
                if process.wait(timeout=120):
                    errors.seek(0)
                    raise SnapshotError('Native Windows cold read failed: ' + errors.read(4096).decode(errors='replace'))
                return
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
            process.stdout.close()
            errors.close()
        time.sleep((0.2, 1.0)[attempt])


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
        self.appended = 0
        self.result = None
        fresh = offset == 0 and not self.path.exists() and not self.path.is_symlink()
        for attempt in range(3):
            self.process, self.errors = _process(path, mode='write', offset=offset,
                                                prefix_sha256=prefix_sha256, reserve_bytes=reserve_bytes)
            ready = self.process.stdout.readline(4096)
            try:
                record = json.loads(ready)
                if record.get('state') != 'native_writer_ready' or record.get('offset') != offset:
                    raise SnapshotError('Native writer readiness differs; no input sent')
                break
            except (ValueError, SnapshotError) as error:
                if self.process.poll() is None:
                    self.process.terminate()
                code = self.process.wait(timeout=30)
                self.errors.seek(0)
                stderr = self.errors.read(4096)
                self.process.stdin.close()
                self.process.stdout.close()
                self.errors.close()
                # The known WSL startup failure occurs before the script and
                # before any input. Retry only an absent fresh destination;
                # never replay an append, created partial or started writer.
                if (attempt < 2 and fresh and not ready and code
                        and retryable_interop_startup(stderr)
                        and not self.path.exists() and not self.path.is_symlink()):
                    time.sleep((0.2, 1.0)[attempt])
                    continue
                raise SnapshotError('Native Windows writer not ready: '
                                    + stderr.decode(errors='replace')) from error

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
