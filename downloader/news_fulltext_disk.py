"""Nonblocking, bounded checks of the actual backing space for the news owner.

WSL's ext4 free blocks are not proof that the dynamically growing VHDX has
physical room.  Reuse the authority's existing backing-volume verifier.  A
failed, pending or stale observation never grants a new acquisition.
"""
from __future__ import annotations

from pathlib import Path
import os
import re
import signal
import subprocess
import sys
import threading
import time

from stockagent.data.news_disk_contract import BACKING_SAMPLE_MAX_AGE_SECONDS


def uses_wsl() -> bool:
    try:
        return 'microsoft' in Path('/proc/sys/kernel/osrelease').read_text().lower()
    except OSError:
        # Unknown OS identity cannot certify the backing volume.
        return True


def _backing_free(root: Path) -> int:
    from stockagent.data_sync.cache_capacity import _admit_backing_volume
    result = _admit_backing_volume(root, 0)
    value = result.get('physical_free_bytes')
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError('backing-volume sample has no bounded byte count')
    return value


class _IsolatedBackingProbe:
    """Keep a wedged native capacity check from wedging the news owner.

    The trusted verifier can block outside Python's own timeout handling (for
    example while acquiring its process-wide WSL interop lock).  Run it in a
    separate session so the owner can kill a timed-out check and try again.
    A child which has not actually exited is retained and prevents another
    launch; failure or an old result never admits source requests.
    """

    def __init__(self, *, timeout_seconds=120, termination_grace_seconds=3):
        if not 0 < timeout_seconds <= 3600 or not 0 < termination_grace_seconds <= 60:
            raise ValueError('invalid backing probe deadline')
        self.timeout = timeout_seconds
        self.termination_grace = termination_grace_seconds
        self._terminating = None

    def __call__(self, root: Path) -> int:
        if self._terminating is not None:
            if self._terminating.poll() is None:
                raise TimeoutError('previous backing probe has not exited')
            if self._terminating.stdout is not None:
                self._terminating.stdout.close()
            self._terminating = None
        # Executing the module from the checkout root keeps the same Python
        # interpreter and verifier as the owner, without mutating its process
        # environment or passing provider credentials on the command line.
        process = subprocess.Popen(
            [sys.executable, '-m', 'downloader.news_fulltext_disk', '--probe', str(root)],
            cwd=Path(__file__).resolve().parents[1], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            output, _ = process.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired as error:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                try:
                    process.kill()
                except OSError:
                    pass
            try:
                process.communicate(timeout=self.termination_grace)
            except subprocess.TimeoutExpired:
                # Do not pile up additional checks if the OS cannot reap one.
                self._terminating = process
            raise TimeoutError('backing probe exceeded its deadline') from error
        if process.returncode or not re.fullmatch(rb'[0-9]{1,19}\n', output):
            raise ValueError('backing probe did not return a valid observation')
        value = int(output)
        if value >= 2**63:
            raise ValueError('backing probe observation is out of bounds')
        return value


class BackingDiskSampler:
    """One daemon probe at a time; a good sample expires after a bounded age."""

    def __init__(self, root: Path, *, interval_seconds=60, max_age_seconds=BACKING_SAMPLE_MAX_AGE_SECONDS,
                 probe=None, clock=None, wsl=None, probe_timeout_seconds=120):
        self.root = Path(root).absolute()
        self.interval = interval_seconds
        self.max_age = max_age_seconds
        self.probe = probe or _IsolatedBackingProbe(timeout_seconds=probe_timeout_seconds)
        self.clock = clock or time.monotonic
        self.wsl = uses_wsl() if wsl is None else wsl
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._inflight = False
        self._next_probe = 0.0
        self._value = None
        self._observed_start = None
        self._failed = False

    def _run(self, started):
        try:
            value = self.probe(self.root)
            if type(value) is not int or not 0 <= value < 2**63:
                raise ValueError('invalid backing-space observation')
        except Exception:
            value = None
        with self._lock:
            self._value = value
            self._failed = value is None
            self._observed_start = started
            self._inflight = False
            self._done.set()

    def sample(self, filesystem_free: int) -> dict:
        if type(filesystem_free) is not int or not 0 <= filesystem_free < 2**63:
            raise ValueError('invalid filesystem free byte count')
        if not self.wsl:
            return {'state': 'not_required', 'free_bytes': filesystem_free, 'inflight': False,
                    'filesystem_free_bytes': filesystem_free, 'backing_free_bytes': None}
        now = self.clock()
        with self._lock:
            if not self._inflight and now >= self._next_probe:
                self._inflight = True
                self._next_probe = now + self.interval
                self._done.clear()
                try:
                    threading.Thread(target=self._run, args=(now,), name='news-backing-space', daemon=True).start()
                except Exception:
                    self._inflight = False
                    self._value = None
                    self._observed_start = now
                    self._failed = True
                    self._done.set()
            age = None if self._observed_start is None else max(0.0, now - self._observed_start)
            if self._value is not None and age is not None and age <= self.max_age:
                return {'state': 'available', 'free_bytes': min(filesystem_free, self._value), 'inflight': self._inflight,
                        'filesystem_free_bytes': filesystem_free, 'backing_free_bytes': self._value,
                        'backing_age_seconds': age}
            state = ('stale' if self._value is not None else 'unavailable' if self._failed else 'pending')
            return {'state': state, 'free_bytes': None, 'inflight': self._inflight, 'filesystem_free_bytes': filesystem_free,
                    'backing_free_bytes': None, 'backing_age_seconds': age}

    def wait(self, timeout: float) -> bool:
        """Wake an initial disk wait when its one in-flight check completes."""
        return self._done.wait(timeout)


if __name__ == '__main__':
    # The parent admits only one bounded integer; errors are not observations.
    if len(sys.argv) != 3 or sys.argv[1] != '--probe':
        raise SystemExit(2)
    try:
        print(_backing_free(Path(sys.argv[2])))
    except Exception:
        raise SystemExit(1)
