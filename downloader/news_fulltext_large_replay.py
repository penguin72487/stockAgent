"""Non-blocking lifetime for the retained-source proof subprocess.

The existing queue owner remains the only database and publication writer.
The child only returns an untrusted proof for the owner to revalidate.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


MAX_REQUEST_BYTES = 4096
MAX_FAILURE_BYTES = 4096
STALL_SECONDS = 120
UNKNOWN_PROGRESS_INITIAL_SECONDS = 900
UNKNOWN_PROGRESS_MAX_SECONDS = 8 * 60 * 60
TERMINATE_GRACE_SECONDS = 5
REPO = Path(__file__).resolve().parents[1]


def _progress(pid):
    """Read child CPU and I/O progress; unavailable observations remain unknown."""
    try:
        stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        cpu = (int(stat[11]), int(stat[12]))
        fields = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/io').read_text().splitlines())
        return cpu + (int(fields['rchar']), int(fields['wchar']),
                      int(fields['read_bytes']), int(fields['write_bytes']))
    except (OSError, ValueError, IndexError, KeyError):
        return None


class LargeReplayJob:
    """One child; polling never waits for source I/O or parsing."""

    def __init__(self, request, *, clock=time.monotonic,
                 unknown_stall_seconds=UNKNOWN_PROGRESS_INITIAL_SECONDS):
        if (type(unknown_stall_seconds) not in (int, float)
                or not 0 < unknown_stall_seconds <= UNKNOWN_PROGRESS_MAX_SECONDS):
            raise ValueError('invalid_unknown_progress_timeout')
        self.request = dict(request, parent_pid=os.getpid())
        raw = json.dumps(self.request, separators=(',', ':')).encode()
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError('large_replay_request_too_large')
        self.clock = clock
        self.output = tempfile.TemporaryFile(mode='w+b')
        self.error = tempfile.TemporaryFile(mode='w+b')
        self.process = None
        self.last_progress = self.clock()
        self.last_observation = None
        self.unknown_since = None
        self.unknown_stall_seconds = unknown_stall_seconds
        self.termination_at = None
        self.termination_error = None
        try:
            self.process = subprocess.Popen([sys.executable, '-m', 'downloader.news_fulltext_large_replay_worker'],
                stdin=subprocess.PIPE, stdout=self.output, stderr=self.error, cwd=REPO)
            self.process.stdin.write(raw)
            self.process.stdin.close()
        except Exception:
            self.close()
            raise

    def poll(self):
        """None is still running; a result is detached and fully reaped."""
        version = self.request.get('version', 1)
        status = self.process.poll()
        if status is None:
            now = self.clock()
            if self.termination_at is not None:
                if now - self.termination_at >= TERMINATE_GRACE_SECONDS:
                    try:
                        self.process.kill()
                    except ProcessLookupError:
                        pass
                return None
            observation = _progress(self.process.pid)
            reason = None
            if observation is None:
                # Missing /proc evidence is not proof of no CPU or I/O. Bound
                # a continuously unknown observation separately so a reader
                # cannot monopolize this one worker forever.
                if self.unknown_since is None:
                    self.unknown_since = now
                if now - self.unknown_since >= self.unknown_stall_seconds:
                    reason = 'worker_progress_unknown_timeout'
            else:
                self.unknown_since = None
                if observation != self.last_observation:
                    self.last_observation = observation
                    self.last_progress = now
                elif now - self.last_progress >= STALL_SECONDS:
                    reason = 'worker_progress_unchanged_timeout'
            if reason:
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass  # The child exited after poll; reap it on the next tick.
                self.termination_at = now
                self.termination_error = reason
            return None
        try:
            if self.termination_at is not None:
                return {'version': version, 'state': 'local_io_retry', 'error': self.termination_error}
            if status != 0:
                # Some native Arrow allocations abort instead of reaching the
                # Python MemoryError handler. Recognize only its explicit
                # allocation failure, never turn arbitrary crashes into OOM.
                if status == -signal.SIGABRT:
                    self.error.seek(0, os.SEEK_END)
                    size = self.error.tell()
                    self.error.seek(max(0, size - MAX_FAILURE_BYTES))
                    if b'std::bad_alloc' in self.error.read(MAX_FAILURE_BYTES):
                        return {'version': version, 'state': 'resource_wait', 'error': 'worker_memory_exhausted'}
                return {'version': version, 'state': 'local_io_retry', 'error': 'worker_process_failed'}
            self.output.seek(0, os.SEEK_END)
            if self.output.tell() > max(MAX_FAILURE_BYTES, self.request['max_output_bytes']):
                return {'version': version, 'state': 'resource_wait', 'error': 'worker_output_too_large'}
            self.output.seek(0)
            try:
                result = json.load(self.output)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                return {'version': version, 'state': 'local_io_retry', 'error': 'worker_output_invalid'}
            if not isinstance(result, dict) or result.get('version') != version:
                return {'version': version, 'state': 'local_io_retry', 'error': 'worker_output_invalid'}
            return result
        finally:
            self.close()

    def close(self):
        # On normal completion poll already reaped the process. On explicit
        # owner shutdown terminate just this child; pdeathsig covers crashes.
        if self.process is not None and self.process.poll() is None:
            try:
                self.process.terminate()
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    self.process.kill()
                except ProcessLookupError:
                    pass
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
        self.output.close()
        self.error.close()
