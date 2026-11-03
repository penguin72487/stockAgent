"""Bounded binary pipes to native Windows FileStream, not WSL 9p data I/O.

Only the enrolled D cold namespace and guarded backup/completed staging are allowed. Canonical authority, locking,
manifests and inode checks remain in Linux; this is solely a byte I/O adapter.
Non-D paths retain normal file I/O (including unit-test temporary directories).
"""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import os
import re
import selectors
from pathlib import Path, PureWindowsPath
import stat
import subprocess
import tempfile
import threading
import time

from stockagent.data_sync.desync_snapshots import SnapshotError


class WorkspaceCapacityDeferred(SnapshotError):
    """Verified backing capacity is insufficient for optional local staging."""

    def __init__(self, message, *, physical_free_bytes=None, required_bytes=None):
        super().__init__(message)
        self.physical_free_bytes = physical_free_bytes
        self.required_bytes = required_bytes


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts/windows_cold_binary_io.ps1'
POWERSHELL = '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
ALIASES = (
    (Path('/srv/stockagent-packed'), 'stockagent-cold-primary/packed'),
    (Path('/srv/stockagent-d-volume/stockagent-cold-primary'), 'stockagent-cold-primary'),
    (Path('/mnt/d/stockagent-cold-primary'), 'stockagent-cold-primary'),
    (Path('/srv/stockagent-backup-ingress-lab203'), 'stockagent-backup-ingress-lab203'),
    (Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203'), 'stockagent-backup-ingress-lab203'),
    (Path('/srv/stockagent-d-volume/stockagent-immutable-lake'), 'stockagent-immutable-lake'),
    (Path('/srv/stockagent-d-volume/stockagent-completed-benchmark-scratch'), 'stockagent-completed-benchmark-scratch'),
    (Path('/srv/stockagent-d-volume/stockagent-completed-offline-scratch'), 'stockagent-completed-offline-scratch'),
    (Path('/srv/stockagent-d-volume/stockagent-legacy-archive-stage'), 'stockagent-legacy-archive-stage'),
)
READ_ONLY_STAGE_PREFIXES = (
    'D:\\stockagent-completed-benchmark-scratch\\',
    'D:\\stockagent-completed-offline-scratch\\',
    'D:\\stockagent-legacy-archive-stage\\',
)

_INTEROP_DIRECTORY = Path('/run/WSL')
_INTEROP_LOCK = threading.Lock()
_INTEROP_CACHE = None
_INTEROP_MARKER = b'stockagent-cold-interop-v1'
# The admission executes an empty batch only. A busy Windows host can need
# over 10 s to start an otherwise healthy bridge; do not classify that as a
# failed endpoint before allowing a bounded startup interval.
_INTEROP_PROBE_TIMEOUT_SECONDS = 15
MAX_HASH_BATCH_MEMBERS = 128
HASH_METADATA_WORKERS = 16


def _interop_socket(path):
    """Admit local WSL init sockets, including its fixed 1 -> 2 alias."""
    try:
        path = Path(path)
        parent = _INTEROP_DIRECTORY.lstat()
        if (path.parent != _INTEROP_DIRECTORY or _INTEROP_DIRECTORY.is_symlink()
                or parent.st_uid != 0 or parent.st_mode & 0o022
                or re.fullmatch(r'[1-9][0-9]*_interop', path.name) is None):
            return None
        entry = path.lstat()
        if stat.S_ISLNK(entry.st_mode):
            if (path.name != '1_interop' or entry.st_uid != 0
                    or os.readlink(path) != str(_INTEROP_DIRECTORY / '2_interop')):
                return None
            path = _INTEROP_DIRECTORY / '2_interop'
            entry = path.lstat()
        pid = path.name.removesuffix('_interop')
        if (not stat.S_ISSOCK(entry.st_mode) or entry.st_uid != os.geteuid()
                or Path('/proc', pid, 'exe').resolve(strict=True) != Path('/init')):
            return None
        return str(path), entry.st_dev, entry.st_ino
    except (OSError, RuntimeError, TypeError, ValueError):
        return None


def _windows_process_environment():
    """Validate a bridge before opening bytes; never replay a failed write.

    A socket's existence does not prove its Windows process context still
    works. Run the canonical encoded batch with an empty member list, then
    require its exact marker, at most eight probes across trusted init endpoints.
    A timed-out empty admission can retry within that same budget; no target
    file configuration has been sent. Cache that
    result briefly. An expired, unchanged verified endpoint is tried first,
    but must pass a new empty probe. Each caller keeps its own env so concurrent
    activities cannot change one another's transport context.
    """
    global _INTEROP_CACHE
    if not _INTEROP_DIRECTORY.is_dir():
        return None
    with _INTEROP_LOCK:
        now = time.monotonic()
        previous_verified = None
        if _INTEROP_CACHE is not None:
            signature, checked = _INTEROP_CACHE
            if _interop_socket(signature[0]) == signature:
                previous_verified = Path(signature[0])
                if now - checked < 60:
                    return {**os.environ, 'WSL_INTEROP': signature[0]}
        preferred = os.environ.get('WSL_INTEROP')
        paths = [previous_verified] if previous_verified is not None else []
        if preferred:
            paths.append(Path(preferred))
        paths.extend(sorted(_INTEROP_DIRECTORY.glob('*_interop'),
                            key=lambda p: int(p.name.split('_', 1)[0])
                            if re.fullmatch(r'[1-9][0-9]*_interop', p.name) else -1,
                            reverse=True))
        probe = _native_command([], batched=True) + "\n[Console]::Write('stockagent-cold-interop-v1')"
        encoded_probe = base64.b64encode(probe.encode('utf-16le')).decode()
        seen, candidates, attempts = set(), [], 0
        for path in paths:
            signature = _interop_socket(path)
            if signature is None or signature in seen:
                continue
            seen.add(signature)
            candidates.append(signature)
            if len(candidates) == 8:
                break
        for delay in (0, .25, 1.0):
            if not candidates or attempts >= 8:
                break
            if delay:
                time.sleep(delay)
            pending, candidates = candidates, []
            for signature in pending:
                if _interop_socket(signature[0]) != signature:
                    continue
                attempts += 1
                env = {**os.environ, 'WSL_INTEROP': signature[0]}
                try:
                    result = subprocess.run([POWERSHELL, '-NoLogo', '-NoProfile', '-NonInteractive',
                        '-EncodedCommand', encoded_probe], input=b'[]\n',
                        env=env, capture_output=True, timeout=_INTEROP_PROBE_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired as error:
                    # No target path or payload has left Python. Only an
                    # empty-output admission timeout may retry; malformed
                    # markers and script/permission errors remain rejected.
                    if not error.output:
                        candidates.append(signature)
                    result = None
                except OSError:
                    result = None
                if (result is not None and result.returncode == 0 and result.stdout == _INTEROP_MARKER
                        and _interop_socket(signature[0]) == signature):
                    _INTEROP_CACHE = signature, time.monotonic()
                    return env
                if attempts == 8:
                    break
        _INTEROP_CACHE = None
        raise SnapshotError('No verified local WSL interop bridge; native file I/O not started')


def admit_workspace(path: Path, required_bytes: int, *, windows_distribution: str | None = None) -> dict:
    """Admit native SSD scratch using real backing capacity, without ML imports.

    Shared with the completed-return owner. The old public entrypoint delegates
    here so native byte readers do not import model/training lifecycle modules.
    """
    from stockagent.service_layout import canonical_service_path
    path = canonical_service_path(path)
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise SnapshotError("returned artifact path is redirected")
    if not path.is_dir():
        raise SnapshotError("training return workspace must already exist")
    result = subprocess.run(["findmnt", "-n", "-o", "FSTYPE", "-T", str(path)],
                            capture_output=True, text=True, check=True, timeout=10)
    filesystem = result.stdout.strip()
    if filesystem not in {"ext4", "xfs", "btrfs", "zfs"}:
        raise SnapshotError("authority mutable return state needs a native local filesystem")
    fs = os.statvfs(path)
    free = fs.f_bavail * fs.f_frsize
    if "microsoft" in Path("/proc/sys/kernel/osrelease").read_text().lower():
        from stockagent.data_sync.workspace_volume import ENROLLMENT, check as check_e, configuration
        if ENROLLMENT.exists():
            workspace = Path(configuration()['mount_point'])
            if workspace.is_mount() and workspace.stat().st_dev == path.stat().st_dev:
                # Stable C-facing data aliases may bind E too. Check the
                # filesystem actually holding these bytes, not the C process.
                measured = check_e(physical=True)
                free = min(free, measured['physical_free_bytes'])
                if free < required_bytes:
                    raise WorkspaceCapacityDeferred('E workspace lacks real physical free space',
                        physical_free_bytes=free, required_bytes=required_bytes)
                return {'filesystem': filesystem, 'physical_free_bytes': free,
                        'required_bytes': required_bytes, 'physical_volume': 'E:'}
        from stockagent.data_sync.recovery_queue import physical_free_bytes
        if windows_distribution is not None and (
                not isinstance(windows_distribution, str)
                or re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", windows_distribution) is None):
            raise SnapshotError("explicit WSL backing distribution is invalid")
        config = {"windows_distribution": (os.environ.get("WSL_DISTRO_NAME", "")
                                           if windows_distribution is None else windows_distribution),
                  "powershell": POWERSHELL}
        free = min(free, physical_free_bytes(config))
    if free < required_bytes:
        raise WorkspaceCapacityDeferred("training return workspace lacks real physical free space",
                                        physical_free_bytes=free, required_bytes=required_bytes)
    return {"filesystem": filesystem, "physical_free_bytes": free, "required_bytes": required_bytes}


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
    # A transport relocated to E ext4 has no D Windows pathname. Source D
    # reads still use the native byte adapter; the destination uses Linux I/O.
    if path == Path('/srv/stockagent-backup-ingress-lab203') or Path('/srv/stockagent-backup-ingress-lab203') in path.parents:
        from stockagent.data_sync.transport_placement import on_e
        if on_e():
            return None
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


def _configuration(path, **options):
    if windows_path(path) is None:
        raise SnapshotError('Windows binary I/O requires fixed D cold path')
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    return _configuration_on_checked_mount(path, **options)


def _configuration_on_checked_mount(
    path, *, mode, offset=0, prefix_sha256=None, reserve_bytes=32 * 1024**3,
    source=None, expected_sha256=None, expected_bytes=None,
):
    """Build an exact member config inside a freshly guarded native operation."""
    native = windows_path(path)
    if native is None:
        raise SnapshotError('Windows binary I/O requires fixed D cold path')
    if native.startswith('D:\\stockagent-backup-ingress-lab203\\'):
        from stockagent.data_sync.packed_backup import mounted_volume
        physical = Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203')
        if (mounted_volume(Path('/srv/stockagent-d-volume'))[2] != 'D:'
                or not physical.samefile(Path('/srv/stockagent-backup-ingress-lab203'))):
            raise SnapshotError('Native transport differs from its enrolled D backing volume')
    if native.startswith('D:\\stockagent-immutable-lake\\'):
        from stockagent.data_sync.packed_backup import mounted_volume
        mount = Path('/srv/stockagent-d-volume')
        if mode != 'hash' or mounted_volume(mount)[2] != 'D:' or Path(path).stat().st_dev != mount.stat().st_dev:
            raise SnapshotError('Native lake hash requires its enrolled D volume and read-only mode')
    if native.startswith(READ_ONLY_STAGE_PREFIXES):
        from stockagent.data_sync.packed_backup import mounted_volume
        mount = Path('/srv/stockagent-d-volume')
        legacy_encoding = native.startswith('D:\\stockagent-legacy-archive-stage\\')
        if (mode not in (('read', 'hash') if legacy_encoding else ('read',)) or mounted_volume(mount)[2] != 'D:'
                or Path(path).stat().st_dev != mount.stat().st_dev):
            raise SnapshotError('Native completed-stage read requires its actual D volume and read-only mode')
        if legacy_encoding:
            from stockagent.data_sync.legacy_artifact_archive import reviewed_retired_stage_path
            if not reviewed_retired_stage_path(Path(path)):
                raise SnapshotError('Native legacy-stage read requires an exact reviewed retired encoding')
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
    if mode == 'read' and expected_bytes is not None:
        if type(expected_bytes) is not int or not 0 <= expected_bytes <= 8 * 1024**3:
            raise SnapshotError('Native exact read exceeds its member bound')
        config['expected_bytes'] = expected_bytes
    return config


def _batch_ready_marker(token):
    if not isinstance(token, str) or re.fullmatch('[0-9a-f]{32}', token) is None:
        raise SnapshotError('Native batch readiness token is invalid')
    return ('stockagent-native-batch-ready-v1:' + token).encode('ascii')


def _native_command(configs, *, batched=False, startup_token=None):
    # No shell interpolation, external script path or inherited plaintext key.
    readiness = b'' if startup_token is None else _batch_ready_marker(startup_token)
    if readiness and not batched:
        raise SnapshotError('Native readiness is only for bounded metadata batches')
    if batched:
        # Metadata goes through stdin so Windows' command-line length does not
        # grow with the batch. The only executable bytes are the fixed script.
        command = "[Console]::InputEncoding = New-Object Text.UTF8Encoding($false)\n"
        if readiness:
            # No file operation can begin before metadata is sent. Flush an
            # exact nonce first so a lost WSL launch cannot consume the full
            # payload I/O deadline while doing no useful work.
            command += "[Console]::Out.Write('" + readiness.decode('ascii') + "' + [char]10)\n"
            command += "[Console]::Out.Flush()\n"
        command += "$batch = [Console]::In.ReadLine() | ConvertFrom-Json\nforeach ($item in $batch) {\n"
        # Keep nested chunk metadata in process memory. An individual Windows
        # environment value is limited to 32,767 characters; a valid original
        # with hundreds of extents exceeds that limit after base64 encoding.
        # A fresh scriptblock scope also prevents per-member state leaking.
        command += '& { param($stockagentBinaryBatchConfig)\n'
        command += SCRIPT.read_text() + '\n} $item\n}\n'
    else:
        encoded_config = base64.b64encode(json.dumps(configs[0]).encode()).decode()
        command = "$env:STOCKAGENT_BINARY_IO_CONFIG='" + encoded_config + "';\n" + SCRIPT.read_text()
    return command


def _launch(configs, *, batched=False, startup_token=None):
    encoded = base64.b64encode(_native_command(configs, batched=batched,
        startup_token=startup_token).encode('utf-16le')).decode()
    environment = _windows_process_environment()
    errors = tempfile.TemporaryFile()
    try:
        process = subprocess.Popen([POWERSHELL, '-NoLogo', '-NoProfile', '-NonInteractive',
                                    '-EncodedCommand', encoded], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=errors, env=environment)
    except BaseException:
        errors.close()
        raise
    return process, errors


def _process(path, **kwargs):
    return _launch([_configuration(path, **kwargs)])


class _BatchNotReady(Exception):
    def __init__(self, reason, received):
        super().__init__(reason)
        self.reason = reason
        self.received = received


def _await_batch_ready(process, token, *, timeout=15.0):
    """Consume only the exact startup line, with no metadata or payload sent."""
    expected = _batch_ready_marker(token) + b'\n'
    received = b''
    deadline = time.monotonic() + timeout
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while len(received) < len(expected):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise _BatchNotReady('timeout', received)
            # Raw reads avoid BufferedReader prefetch stealing the following
            # JSON proofs from communicate(). There is one reader per pipe.
            chunk = os.read(process.stdout.fileno(), len(expected) - len(received))
            if not chunk:
                raise _BatchNotReady('eof', received)
            received += chunk
            if not expected.startswith(received):
                raise _BatchNotReady('invalid-marker', received)


def _communicate_cancellable_hash(process, payload, cancel_event):
    """Stop only a read-only peer after another verifier has failed.

    Repeated communicate calls after TimeoutExpired retain the already sent
    input and buffered output. The original one-hour I/O deadline is unchanged.
    """
    deadline = time.monotonic() + 3600
    while True:
        if cancel_event.is_set():
            raise SnapshotError('Native read-only batch cancelled after peer verification failed')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(process.args, 3600)
        try:
            return process.communicate(payload, timeout=min(1.0, remaining))
        except subprocess.TimeoutExpired:
            payload = None


def _batch(configs, *, cancel_event=None):
    read_only = bool(configs) and all(c['mode'] in {'hash', 'hash-chunks'} for c in configs)
    member_limit = MAX_HASH_BATCH_MEMBERS if read_only else 16
    if (not 1 <= len(configs) <= member_limit or any(c['mode'] not in {'copy', 'hash', 'hash-chunks'} for c in configs)
            or sum(c['expected_bytes'] for c in configs) > 16 * 1024**3):
        raise SnapshotError('Native batch exceeds its immutable I/O bounds')
    if cancel_event is not None and not read_only:
        raise SnapshotError('Cancellation is supported only for read-only native batches')
    for attempt in range(3):
        if cancel_event is not None and cancel_event.is_set():
            raise SnapshotError('Native read-only batch cancelled after peer verification failed')
        token = os.urandom(16).hex()
        process, errors = _launch(configs, batched=True, startup_token=token)
        retry = False
        try:
            try:
                _await_batch_ready(process, token)
            except _BatchNotReady as failure:
                # No metadata has left Python. Only a read-only operation
                # with zero startup bytes may be retried, at most twice.
                if (read_only and not failure.received
                        and failure.reason in {'timeout', 'eof'} and attempt < 2):
                    retry = True
                else:
                    raise SnapshotError(f'Native batch failed (startup-{failure.reason}, '
                        f'output_bytes={len(failure.received)}); no metadata sent') from failure
            if not retry:
                payload = json.dumps(configs, ensure_ascii=True).encode() + b'\n'
                if cancel_event is None:
                    output, _ = process.communicate(payload, timeout=3600)
                else:
                    output, _ = _communicate_cancellable_hash(process, payload, cancel_event)
                if process.returncode or len(output) > max(16 * 1024, len(configs) * 512):
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
            if process.stdin is not None:
                process.stdin.close()
            process.stdout.close();errors.close()
        if retry:
            time.sleep((0.2, 1.0)[attempt])


def _stable(before, after):
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_mode', 'st_nlink')
    return all(getattr(before, key) == getattr(after, key) for key in fields)


def _copy_reserve(reserve_bytes):
    if type(reserve_bytes) is not int or not 32 * 1024**3 <= reserve_bytes <= 64 * 1024**3:
        raise SnapshotError('Native backup copy reserve must preserve the 32 GiB writer floor')
    return reserve_bytes


def copy_many(members, *, reserve_bytes=64 * 1024**3):
    """One native process, full per-member copy/hash/flush and source proof."""
    reserve_bytes = _copy_reserve(reserve_bytes)
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
            expected_sha256=sha, expected_bytes=count, reserve_bytes=reserve_bytes))
        signatures.append(before)
    for (source, destination, sha, count), before, proof in zip(members, signatures, _batch(configs), strict=True):
        after = destination.lstat()
        if (not stat.S_ISREG(after.st_mode) or after.st_size != count or not _stable(before, source.lstat())
                or proof.get('flushed') is not True or proof.get('bytes') != count or proof.get('sha256') != sha):
            raise SnapshotError('Native batch changed its source or exact copied bytes')
        destination.chmod(0o600)


def hash_many(paths, *, cancel_event=None):
    paths = [Path(p) for p in paths]
    if not 1 <= len(paths) <= MAX_HASH_BATCH_MEMBERS or len(set(paths)) != len(paths):
        raise SnapshotError('Native hash batch members are repeated or unbounded')
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    # No persistent/TTL mount cache: each bounded batch has fresh before/after
    # authority guards. All member path, full SHA and signature checks remain.
    # Re-running the shell/Python mount checker for every member dominated
    # small-object recovery despite the already batched Windows process.
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    def prepare(path):
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise SnapshotError('Native batch hash needs regular exact members')
        return before, _configuration_on_checked_mount(path, mode='hash', expected_bytes=before.st_size)
    # Every member still traverses its path and captures its own generation.
    # Overlap independent 9p metadata round trips; do not cache permissions or
    # start native I/O before all member checks have joined successfully.
    with ThreadPoolExecutor(max_workers=min(len(paths), HASH_METADATA_WORKERS),
                            thread_name_prefix='native-hash-member') as pool:
        prepared = list(pool.map(prepare, paths))
        signatures = [before for before, _ in prepared]
        configs = [config for _, config in prepared]
        proofs = _batch(configs) if cancel_event is None else _batch(configs, cancel_event=cancel_event)
        _check_d_primary_mount(Path('/srv/stockagent-packed'))
        after = list(pool.map(Path.lstat, paths))
    result = {}
    for path, before, final, proof in zip(paths, signatures, after, proofs, strict=True):
        if (not _stable(before, final) or proof.get('bytes') != before.st_size
                or not isinstance(proof.get('sha256'), str) or not re.fullmatch('[0-9a-f]{64}', proof['sha256'])):
            raise SnapshotError('Native batch full hash changed an exact member')
        result[path] = proof['sha256']
    return result


def copy_verified(source, destination, *, expected_sha256, expected_bytes, reserve_bytes=64 * 1024**3):
    """Native D-to-D copy/hash/flush; Linux retains all authority/signature gates."""
    reserve_bytes = _copy_reserve(reserve_bytes)
    source, destination = Path(source), Path(destination)
    before = source.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes:
        raise SnapshotError('Native copy source is not the exact regular member')
    destination.parent.mkdir(parents=True, exist_ok=True)
    process, errors = _process(destination, mode='copy', source=source,
        expected_sha256=expected_sha256, expected_bytes=expected_bytes, reserve_bytes=reserve_bytes)
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
def binary_reader_many(members):
    """Stream ordered exact members through one bounded native process.

    Repeated extents remain repeated bytes. The caller must consume the entire
    stream and verify each member's SHA; no member is accepted before this
    context's process, fresh authority and before/after signature checks pass.
    Only an empty, known interop startup failure can retry, never partial data.
    """
    members = [(Path(path), count) for path, count in members]
    if (not 1 <= len(members) <= 16
            or any(type(count) is not int or not 0 <= count <= 8 * 1024**3 for _, count in members)
            or sum(count for _, count in members) > 16 * 1024**3):
        raise SnapshotError('Native read batch exceeds its exact immutable bounds')
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    for attempt in range(3):
        _check_d_primary_mount(Path('/srv/stockagent-packed'))
        signatures = [path.lstat() for path, _ in members]
        if any(not stat.S_ISREG(info.st_mode) or info.st_size != count
               for (_, count), info in zip(members, signatures, strict=True)):
            raise SnapshotError('Native read batch differs from an exact regular member')
        configs = [_configuration_on_checked_mount(path, mode='read', expected_bytes=count)
                   for path, count in members]
        process, errors = _launch(configs, batched=True)
        retry = False
        try:
            try:
                process.stdin.write(json.dumps(configs, ensure_ascii=True).encode() + b'\n')
                process.stdin.close()
            except BrokenPipeError:
                # Classify the real child failure below, never retry blindly.
                if not process.stdin.closed:
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
            if not process.stdout.peek(1) and process.wait(timeout=30):
                errors.seek(0)
                failure = errors.read(4096)
                if attempt < 2 and retryable_interop_startup(failure):
                    retry = True
                else:
                    raise SnapshotError('Native read batch startup failed: ' + failure.decode(errors='replace'))
            if not retry:
                yield process.stdout
                if process.stdout.read(1):
                    raise SnapshotError('Native read batch has unconsumed or excess bytes')
                if process.wait(timeout=120):
                    errors.seek(0)
                    raise SnapshotError('Native read batch failed: ' + errors.read(4096).decode(errors='replace'))
                _check_d_primary_mount(Path('/srv/stockagent-packed'))
                if any(not _stable(before, path.lstat())
                       for (path, _), before in zip(members, signatures, strict=True)):
                    raise SnapshotError('Native read batch changed an exact member')
                return
        finally:
            if not process.stdin.closed:
                process.stdin.close()
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
            process.stdout.close()
            errors.close()
        time.sleep((0.2, 1.0)[attempt])


@contextmanager
def binary_reader(path, *, expected_bytes=None):
    path = Path(path)
    if windows_path(path) is None:
        with path.open('rb') as stream:
            yield stream
        return
    for attempt in range(3):
        process, errors = _process(path, mode='read',
            **({'expected_bytes': expected_bytes} if expected_bytes is not None else {}))
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
        if (type(offset) is not int or offset < 0
                or (offset and prefix_sha256 is None)
                or (prefix_sha256 is not None and (not isinstance(prefix_sha256, str)
                                                   or re.fullmatch(r'[a-f0-9]{64}', prefix_sha256) is None))):
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
