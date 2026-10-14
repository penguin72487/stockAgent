"""Bounded liveness evidence for the existing, single-flight source owner.

A live child or a recently observed writer lock is not data progress/readiness.
It only prevents a shorter inference watchdog from killing work whose source
timeout has not expired. Entries exist solely while their real owner is waiting;
their deadlines never slide with heartbeats or output. No worker is created here.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import math
import subprocess
import threading
import time
from typing import IO, Iterator, Sequence


@dataclass
class _SourceOperation:
    operation_id: int
    started: float
    deadline: float
    thread: threading.Thread
    process: subprocess.Popen | None = None
    lock_handle: IO | None = None
    last_blocked: float | None = None


class SourceOperationRegistry:
    """Keep tiny, process-local proofs indexed by the resolved shared command."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._operations: dict[tuple[str, ...], _SourceOperation] = {}
        self._sequence = 0

    @contextmanager
    def _watch(self, command: Sequence[str], *, timeout_seconds: float,
               process: subprocess.Popen | None = None,
               lock_handle: IO | None = None) -> Iterator[_SourceOperation]:
        timeout = float(timeout_seconds)
        if not command or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("a resolved command and positive finite timeout are required")
        key = tuple(command)
        started = time.monotonic()
        with self._lock:
            if key in self._operations:
                raise RuntimeError("source operation must use the existing single-flight lock")
            self._sequence += 1
            operation = _SourceOperation(
                self._sequence, started, started + timeout,
                threading.current_thread(), process, lock_handle,
                started if lock_handle is not None else None,
            )
            self._operations[key] = operation
        try:
            yield operation
        finally:
            with self._lock:
                if self._operations.get(key) is operation:
                    self._operations.pop(key)

    def watch_process(self, command: Sequence[str], process: subprocess.Popen,
                      *, timeout_seconds: float):
        return self._watch(command, timeout_seconds=timeout_seconds, process=process)

    def watch_writer_lock(self, command: Sequence[str], handle: IO,
                          *, timeout_seconds: float):
        # Caller enters only after LOCK_NB actually raised BlockingIOError.
        return self._watch(command, timeout_seconds=timeout_seconds, lock_handle=handle)

    def observe_blocked(self, operation: _SourceOperation) -> None:
        """Record a real failed LOCK_NB probe, never advance the fixed deadline."""
        with self._lock:
            operation.last_blocked = time.monotonic()

    def snapshot(self, command: Sequence[str], *, now: float | None = None) -> dict | None:
        observed = time.monotonic() if now is None else float(now)
        key = tuple(command)
        with self._lock:
            operation = self._operations.get(key)
            if operation is None:
                return None
            last_blocked = operation.last_blocked
        if (not math.isfinite(observed) or observed < operation.started
                or observed >= operation.deadline or not operation.thread.is_alive()):
            return None
        process = operation.process
        if process is not None:
            if process.poll() is not None:
                return None
            kind, pid = "source_subprocess", process.pid
        else:
            handle = operation.lock_handle
            if (handle is None or handle.closed or last_blocked is None
                    or not 0 <= observed - last_blocked <= 2.5):
                return None
            kind, pid = "verified_writer_lock_wait", None
        with self._lock:
            if self._operations.get(key) is not operation:
                return None
        return {
            "operation_id": operation.operation_id,
            "kind": kind,
            "pid": pid,
            "started_monotonic": operation.started,
            "deadline_monotonic": operation.deadline,
            "remaining_seconds": max(0.0, operation.deadline - observed),
            "source_ready": False,
        }
