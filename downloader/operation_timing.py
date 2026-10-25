"""Small invocation-local clocks; observations never participate in data identity."""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
import time


CONTRACT = "source_operation_monotonic_v1"
OPENBB_META_PREFIX = "operation_timing_v1:"


class OperationTiming:
    """Retain inclusive stages, including exceptions, without changing control flow.

    A snapshot ends before its own serialization/persistence. Stages can overlap
    the call total and each other; they must not be summed as elapsed wall time.
    No URLs, credentials, source values or exception messages are collected.
    """

    def __init__(self):
        self.started_ns = time.perf_counter_ns()
        self.stages = []
        self.invalid_clock = False

    @contextmanager
    def measure(self, stage, *, provider=None):
        started = time.perf_counter_ns()
        outcome = "returned"
        try:
            yield
        except BaseException:
            outcome = "raised"
            raise
        finally:
            elapsed = time.perf_counter_ns() - started
            if elapsed < 0:
                self.invalid_clock = True
            else:
                row = {"stage": stage, "seconds": elapsed / 1e9, "outcome": outcome}
                if provider is not None:
                    row["provider"] = provider
                self.stages.append(row)

    def call(self, stage, function, /, *args, timing_provider=None, **kwargs):
        with self.measure(stage, provider=timing_provider):
            return function(*args, **kwargs)

    @contextmanager
    def enter(self, stage, context, *, provider=None):
        # ExitStack preserves the wrapped context's suppression/exception and
        # release behavior while the clock covers only __enter__, not its body.
        with ExitStack() as stack:
            with self.measure(stage, provider=provider):
                value = stack.enter_context(context)
            yield value

    def snapshot(self, scope):
        elapsed = time.perf_counter_ns() - self.started_ns
        return {"contract": CONTRACT, "clock": "perf_counter_ns", "scope": scope,
                "seconds": elapsed / 1e9 if elapsed >= 0 else None,
                "invalid_clock": self.invalid_clock or elapsed < 0,
                "stages": [dict(row) for row in self.stages],
                "aggregation": "inclusive stages overlap total; do not sum"}
