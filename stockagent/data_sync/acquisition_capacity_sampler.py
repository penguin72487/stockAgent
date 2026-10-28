"""Bounded, nonblocking samples of the existing acquisition capacity gate.

The gate is a dispatch preflight, not a reservation against other writers.
No new provider work is admitted from a missing, failed or stale sample.
"""
from __future__ import annotations

from pathlib import Path
import threading
import time
from typing import Callable

from stockagent.data_sync.cache_capacity import acquisition_dispatch_status


def capacity_retry_delay_seconds(sample: dict) -> int:
    """Revisit an in-flight sample before it can become stale.

    Actual reserve failures keep the slower polling cadence. An unfinished or
    stale physical probe needs a short retry because its completed value has a
    bounded lifetime measured from the start of that probe.
    """
    return 5 if sample.get('reason') in {'capacity_sample_pending', 'capacity_sample_expired'} else 60


class AcquisitionCapacitySampler:
    """Keep one physical-volume probe in flight while consumers dispatch work."""

    def __init__(self, root: Path, *, interval_seconds: float = 15,
                 max_age_seconds: float = 30, probe: Callable | None = None,
                 clock: Callable | None = None):
        if not 0 < interval_seconds < max_age_seconds:
            raise ValueError("capacity sample age must exceed the probe interval")
        self.root = Path(root).absolute()
        self.interval = interval_seconds
        self.max_age = max_age_seconds
        self.probe = probe or acquisition_dispatch_status
        self.clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._inflight = False
        self._next_probe = 0.0
        self._observed_start: float | None = None
        self._value: dict | None = None

    def _run(self, started: float) -> None:
        try:
            result = self.probe(self.root)
            if not isinstance(result, dict) or result.get("state") not in {
                    "admitted", "deferred", "not_enrolled"}:
                raise ValueError("invalid acquisition capacity result")
            # Do not publish workspace paths or arbitrary exception messages.
            result = {key: result[key] for key in (
                "state", "reason", "available_bytes", "required_bytes",
                "minimum_free_bytes", "physical_free_bytes", "required_bytes_including_reserve",
                "guard_kind", "error_type",
            ) if key in result}
        except Exception as error:
            result = {"state": "deferred", "reason": "capacity_probe_failed",
                      "error_type": type(error).__name__}
        with self._lock:
            self._value = result
            # Measure age from probe start, never from a delayed completion.
            self._observed_start = started
            self._inflight = False
            self._done.set()

    def sample(self) -> dict:
        now = self.clock()
        with self._lock:
            if not self._inflight and now >= self._next_probe:
                self._inflight = True
                self._next_probe = now + self.interval
                self._done.clear()
                try:
                    threading.Thread(target=self._run, args=(now,),
                                     name="acquisition-capacity", daemon=True).start()
                except Exception as error:
                    self._inflight = False
                    self._value = {"state": "deferred", "reason": "capacity_probe_failed",
                                   "error_type": type(error).__name__}
                    self._observed_start = now
                    self._done.set()
            age = None if self._observed_start is None else max(0.0, now - self._observed_start)
            if self._value is not None and age is not None and age <= self.max_age:
                return {**self._value, "sample_age_seconds": age, "probe_inflight": self._inflight}
            return {"state": "deferred",
                    "reason": "capacity_sample_pending" if self._value is None else "capacity_sample_expired",
                    "sample_age_seconds": age, "probe_inflight": self._inflight,
                    "guard_kind": "dispatch_preflight_not_write_reservation"}

    def initial_sample(self, timeout_seconds: float = 12) -> dict:
        """Wait for an initial or expired probe before a long dispatch phase."""
        value = self.sample()
        if value.get("reason") in {"capacity_sample_pending", "capacity_sample_expired"}:
            self._done.wait(timeout_seconds)
            value = self.sample()
        return value
