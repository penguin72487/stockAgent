"""Fence the canonical Shioaji minute writer during its C-to-E handoff.

Custom output roots, including the day-trade repair workspace, stay completely
independent. Before the fixed handoff is enrolled this adds no lock or file to
the source, so an in-flight staging generation is not silently invalidated.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.yahoo_workspace_guard import _uses_root, _writer_guard

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT/'data_tw_minute/shioaji_1m'
DESTINATION = (Path('/srv/stockagent-workspace/downloads')/
               'tw-minute-source-stream-e-20261108-v1/data_tw_minute/shioaji_1m')


@contextmanager
def writer_guard(output, *, timeout_seconds: float = 30.0):
    """Share the handoff lock only for the explicitly enrolled raw root."""
    from stockagent.data_sync import workspace_source_handoff as handoff

    output = Path(os.path.abspath(os.fspath(output)))
    source = _uses_root((output,), SOURCE)
    destination = _uses_root((output,), DESTINATION)
    if not source and not destination:
        yield
        return
    registered = handoff.MINUTE_RAW_NAME in handoff.registered_names()
    if not registered:
        if destination:
            raise SnapshotError('minute E staging generation is not an active writer workspace')
        yield
        return
    with _writer_guard(SOURCE, timeout_seconds):
        if destination:
            with handoff.selected_source(handoff.MINUTE_RAW_NAME):
                journal = handoff.read_journal()
                if (not journal or journal.get('state') != 'active'
                        or Path(journal['destination']) != DESTINATION
                        or not handoff.provider_workspace.exact_mount(SOURCE, DESTINATION)):
                    raise SnapshotError('minute E staging generation is not an active writer workspace')
        yield
