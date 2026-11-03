"""Optional, owner-local work counters; never an acceptance or work executor.

The sink is installed by the existing owner. Worker threads return their results
to that owner before counters advance. Consumers must keep these observations
separate from recovery/retirement receipts.
"""
from contextlib import contextmanager
from contextvars import ContextVar


_sink = ContextVar('storage_work_progress_sink', default=None)
_pass = ContextVar('storage_work_progress_pass', default='workflow')


@contextmanager
def observe_work(callback):
    token = _sink.set(callback)
    try:
        yield
    finally:
        _sink.reset(token)


@contextmanager
def work_pass(name):
    token = _pass.set(name)
    try:
        yield
    finally:
        _pass.reset(token)


def report_work(phase, *, done=0, total=None, done_bytes=None, total_bytes=None):
    callback = _sink.get()
    if callback is not None:
        callback({'phase': phase, 'work_pass': _pass.get(),
                  'completed_items': done, 'total_items': total,
                  'completed_bytes': done_bytes, 'total_bytes': total_bytes})
