"""Cache only the exact stock-session decision used by storage ExecCondition.

The canonical calendar still decides each cache miss. A cache hit requires the
same day, boot, clock, source generations and verifier code, and is bounded to
one minute. This never supplies a calendar to the trading services themselves.
"""
from __future__ import annotations

from datetime import datetime, time as wall_time
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time

ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = Path('/run/stockagent-storage-priority')
TTL_SECONDS = 60
LOCK_WAIT_SECONDS = 0.75
CONTRACT = 'stockagent.storage_priority_session_cache.v1'
HOLIDAY = 'twse_api_holidayschedule_holidayschedule.parquet'
ACTUAL = 'twse_taiex_ohlc.parquet'


def _signature(path: Path):
    try:
        entries = (path.lstat(), path.stat())
    except FileNotFoundError:
        return None
    return [[row.st_dev, row.st_ino, row.st_mode, row.st_size, row.st_mtime_ns, row.st_ctime_ns]
            for row in entries]


def _identity(calendar_root: Path):
    paths = (calendar_root, calendar_root/HOLIDAY, calendar_root.parent/HOLIDAY,
             calendar_root/ACTUAL, (calendar_root/ACTUAL).with_suffix('.summary.json'),
             ROOT/'scripts/check_outside_tw_opening_resource_window.py',
             ROOT/'scripts/storage_priority_cache.py', ROOT/'stockagent/live/market_status.py',
             ROOT/'stockagent/live/market_config.py')
    return [_signature(path) for path in paths]


def _secure_directory(root: Path):
    root.mkdir(mode=0o700, parents=False, exist_ok=True)
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077
            or root.resolve(strict=True) != root):
        raise OSError('storage priority cache directory is not private')


def _open_private(path: Path, flags: int):
    fd = os.open(path, flags | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077
                or info.st_nlink != 1 or not os.path.samestat(info, path.lstat())):
            raise OSError('storage priority cache file is not private')
        return fd
    except BaseException:
        os.close(fd)
        raise


def _valid_result(value, local: datetime) -> bool:
    return (isinstance(value, dict) and type(value.get('stock_session')) is bool
            and value.get('protected') is value.get('stock_session')
            and value.get('allowed') is (not value['protected'])
            and value.get('minimum_runway_minutes') == 120
            and value.get('protected_start') == '06:20:00'
            and value.get('window') == '08:20:00..13:30:00 Asia/Taipei'
            and value.get('policy') == 'best_effort_maintenance_must_not_start_during_tw_session'
            and isinstance(value.get('calendar_evidence'), str)
            and (value['calendar_evidence'].startswith('official TWSE schedule as-of')
                 or value['stock_session'] is True and value['calendar_evidence'] ==
                    'receipt-verified TWSE TAIEX actual session')
            and isinstance(value.get('observed_at_taipei'), str)
            and value['observed_at_taipei'][:10] == local.date().isoformat())


def _decision_digest(value) -> str:
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(body).hexdigest()


def evaluate_cached(local: datetime, calendar_root: Path, compute, *, cache_root=CACHE_ROOT):
    """Return the canonical decision, avoiding repeated calendar reads when safe."""
    if (local.tzinfo is None or getattr(local.tzinfo, 'key', None) != 'Asia/Taipei'
            or not wall_time(6, 20) <= local.time().replace(tzinfo=None) < wall_time(13, 30)):
        return compute()
    calendar_root, cache_root = Path(calendar_root).absolute(), Path(cache_root).absolute()
    fd = None
    try:
        _secure_directory(cache_root)
        fd = _open_private(cache_root/'owner.lock', os.O_CREAT | os.O_RDWR)
        deadline = time.monotonic()+LOCK_WAIT_SECONDS
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as error:
                if error.errno not in (errno.EAGAIN, errno.EACCES):
                    raise
                if time.monotonic() >= deadline:
                    os.close(fd)
                    fd = None
                    return compute()  # A stuck cache never blocks the original guard.
                time.sleep(min(0.05, max(0.0, deadline-time.monotonic())))
        identity = _identity(calendar_root)
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        try:
            read_fd = _open_private(cache_root/'decision.json', os.O_RDONLY)
            try:
                if os.fstat(read_fd).st_size > 65536:
                    raise OSError('storage priority cache oversized')
                body = os.read(read_fd, 65537)
            finally:
                os.close(read_fd)
            cached = json.loads(body)
        except (OSError, ValueError, TypeError):
            cached = {}
        mono_age = (time.monotonic_ns()-cached.get('monotonic_ns', -10**30))/1e9 if isinstance(cached, dict) else -1
        wall_age = local.timestamp()-cached.get('wall_epoch', -10**30) if isinstance(cached, dict) else -1
        value = cached.get('decision') if isinstance(cached, dict) else None
        if (isinstance(cached, dict) and cached.get('contract') == CONTRACT and cached.get('boot_id') == boot
                and cached.get('day') == local.date().isoformat() and cached.get('identity') == identity
                and 0 <= mono_age <= TTL_SECONDS and 0 <= wall_age <= TTL_SECONDS
                and _valid_result(value, local) and cached.get('decision_sha256') == _decision_digest(value)
                and _identity(calendar_root) == identity):
            os.close(fd)
            fd = None
            return {**value, 'observed_at_taipei': local.isoformat(timespec='seconds')}
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        if fd is not None:
            os.close(fd)
        return compute()
    try:
        result = compute()
        try:
            if _valid_result(result, local) and _identity(calendar_root) == identity:
                body = json.dumps({'contract': CONTRACT, 'boot_id': boot, 'day': local.date().isoformat(),
                    'identity': identity, 'monotonic_ns': time.monotonic_ns(), 'wall_epoch': local.timestamp(),
                    'decision': result, 'decision_sha256': _decision_digest(result)},
                    ensure_ascii=False, sort_keys=True).encode()
                temporary = cache_root/f'decision.{os.getpid()}.tmp'
                tmp_fd = _open_private(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                try:
                    with os.fdopen(tmp_fd, 'wb') as stream:
                        stream.write(body)
                    os.replace(temporary, cache_root/'decision.json')
                finally:
                    if temporary.exists():
                        temporary.unlink()
        except (OSError, ValueError, TypeError, KeyError, OverflowError):
            pass  # The canonical decision remains authoritative if caching fails.
        return result
    finally:
        if fd is not None:
            os.close(fd)
