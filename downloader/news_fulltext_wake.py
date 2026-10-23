"""Kernel mailbox hints for the existing owner, never source admission.

Linux watches complete writes/atomic moves in the two bounded private inboxes.
The queue still validates and dispatches every request. No worker, polling
thread, article scan, signal handler or new persistent transport is introduced.
Unsupported platforms and broken watches retain the prior interruptible wait.
"""
from __future__ import annotations

import ctypes
import math
import os
import selectors
import struct
import sys
import threading
import time


class OwnerStopEvent(threading.Event):
    """An ordinary stop Event with nonblocking owner-wait notifications."""

    def __init__(self):
        super().__init__()
        self._wake_lock = threading.RLock()
        self._wakers = set()

    def subscribe(self, callback):
        with self._wake_lock:
            self._wakers.add(callback)
        if self.is_set():
            callback()
        return lambda: self._unsubscribe(callback)

    def _unsubscribe(self, callback):
        with self._wake_lock:
            self._wakers.discard(callback)

    def set(self):
        super().set()
        with self._wake_lock:
            callbacks = tuple(self._wakers)
        for callback in callbacks:
            callback()


class OwnerMailboxWake:
    """One selector/three FDs per owner; no source or queue writer of its own."""

    _EVENT = struct.Struct('iIII')
    _CLOSE_WRITE = 0x00000008
    _MOVED_TO = 0x00000080
    _WATCH_LOST = 0x00000400 | 0x00000800 | 0x00008000
    _OVERFLOW = 0x00004000
    _ONLYDIR_NOFOLLOW = 0x01000000 | 0x02000000

    def __init__(self, queue, stop):
        self.queue, self.stop = queue, stop
        self.backend = 'legacy_stop_wait'
        self._selector = None
        self._fds = []
        self._unsubscribe = None
        self._fd_lock = threading.RLock()
        self._stop_write = None

    def _count(self, event):
        self.queue.metrics.count('local_repairs', event)

    def __enter__(self):
        if sys.platform != 'linux' or not isinstance(self.stop, OwnerStopEvent):
            self._count('owner_mailbox_wake_legacy')
            return self
        try:
            from downloader.news_fulltext_repairs import _directory
            directories = (_directory(self.queue.root, 'pending'),
                _directory(self.queue.root, 'candidate_diagnostics', 'pending'))
            libc = ctypes.CDLL(None, use_errno=True)
            init = libc.inotify_init1
            init.argtypes, init.restype = [ctypes.c_int], ctypes.c_int
            add = libc.inotify_add_watch
            add.argtypes, add.restype = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32], ctypes.c_int
            self._inotify = init(os.O_NONBLOCK | os.O_CLOEXEC)
            if self._inotify < 0:
                raise OSError(ctypes.get_errno(), 'inotify_init1')
            self._fds.append(self._inotify)
            mask = self._CLOSE_WRITE | self._MOVED_TO | self._WATCH_LOST | self._ONLYDIR_NOFOLLOW
            for directory in directories:
                if add(self._inotify, os.fsencode(directory), mask) < 0:
                    raise OSError(ctypes.get_errno(), 'inotify_add_watch')
            self._stop_read, self._stop_write = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
            self._fds.extend((self._stop_read, self._stop_write))
            self._selector = selectors.DefaultSelector()
            self._selector.register(self._inotify, selectors.EVENT_READ, 'mailbox')
            self._selector.register(self._stop_read, selectors.EVENT_READ, 'stop')
            self._unsubscribe = self.stop.subscribe(self._notify_stop)
            self.backend = 'linux_inotify'
            self._count('owner_mailbox_wake_inotify')
        except (OSError, ValueError, AttributeError):
            self.close()
            self._count('owner_mailbox_wake_fallback')
        return self

    def _notify_stop(self):
        with self._fd_lock:
            try:
                if self._stop_write is not None:
                    os.write(self._stop_write, b'\0')
            except OSError:
                pass  # A full pipe is already readable; closed subscriptions are harmless.

    def has_pending(self, *, include_repairs=False):
        from downloader.news_fulltext_candidate_diagnostics import has_unadmitted_candidates
        from downloader.news_fulltext_repairs import _directory, _pending
        try:
            # Repair files are edge hints only. A failed archive may leave one
            # present after its job is complete; that must not force an empty
            # cycle repeatedly. Candidate admission has its indexed job guard.
            return (has_unadmitted_candidates(self.queue) or include_repairs
                and bool(_pending(_directory(self.queue.root, 'pending'))))
        except (OSError, ValueError):
            return False  # The normal owner retains malformed-input handling.

    def _drain(self):
        relevant = False
        for _ in range(8):  # A hot/untrusted inbox cannot monopolize the owner.
            try:
                payload = os.read(self._inotify, 65536)
            except BlockingIOError:
                break
            if not payload:
                raise OSError('inotify_eof')
            offset = 0
            while offset + self._EVENT.size <= len(payload):
                _, mask, _, length = self._EVENT.unpack_from(payload, offset)
                end = offset + self._EVENT.size + length
                if end > len(payload):
                    raise OSError('inotify_partial_event')
                name = payload[offset+self._EVENT.size:end].split(b'\0', 1)[0]
                if mask & self._WATCH_LOST:
                    raise OSError('inotify_watch_lost')
                if mask & self._OVERFLOW:
                    self._count('owner_mailbox_wake_overflow')
                relevant |= bool(mask & self._OVERFLOW or
                    mask & (self._CLOSE_WRITE | self._MOVED_TO) and name.endswith(b'.json'))
                offset = end
        return relevant

    def wait(self, seconds, *, enabled=True):
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds < 0:
            raise ValueError('invalid_owner_mailbox_wait')
        if not enabled or self.backend != 'linux_inotify':
            return self.stop.wait(seconds)
        deadline = time.monotonic()+seconds
        while not self.stop.is_set():
            try:
                events = self._selector.select(max(0, deadline-time.monotonic()))
                if not events:
                    return False
                for key, _ in events:
                    if key.data == 'stop':
                        # Event.clear() remains usable by library clients.
                        try:
                            os.read(self._stop_read, 65536)
                        except BlockingIOError:
                            pass
                    elif self._drain() and self.has_pending(include_repairs=True):
                        self._count('owner_mailbox_input_wakeups')
                        return True
            except (OSError, ValueError):
                self.close()
                self._count('owner_mailbox_wake_fallback')
                return self.stop.wait(max(0, deadline-time.monotonic()))
            if time.monotonic() >= deadline:
                return False
        return True

    def close(self):
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        if self._selector is not None:
            self._selector.close()
            self._selector = None
        with self._fd_lock:
            for fd in self._fds:
                os.close(fd)
            self._fds.clear()
            self._stop_write = None
        self.backend = 'legacy_stop_wait'

    def __exit__(self, *args):
        self.close()
