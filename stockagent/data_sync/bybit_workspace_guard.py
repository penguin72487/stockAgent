"""Fence known Bybit writers during a reviewed C-to-E handoff.

Unenrolled sources and custom roots are unchanged. The shared root lock
preserves concurrent writers and allows the existing handoff owner to acquire
an exclusive fence. An E staging path is never a writer workspace on its own.
"""
from __future__ import annotations

from contextlib import contextmanager
import math
import os
from pathlib import Path

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.yahoo_workspace_guard import _uses_root, _writer_guard

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'service_data/stockagent-live/data_bybit'
ALIASES = (ROOT / 'data_bybit', Path('/srv/stockagent-live/data_bybit'))
DESTINATION = (Path('/srv/stockagent-workspace/downloads') /
               'bybit-source-stream-e-20261108-v1/service_data/stockagent-live/data_bybit')


def _validate_aliases(aliases):
    for alias in aliases:
        try:
            if alias.resolve(strict=True) != SOURCE:
                raise SnapshotError('Bybit canonical alias no longer identifies its registered source')
        except OSError as error:
            raise SnapshotError('Bybit canonical alias is unavailable') from error


@contextmanager
def writer_guard(*paths, timeout_seconds: float = 180.0):
    """Guard only the canonical alias, its physical root, or exact E stage."""
    from stockagent.data_sync import workspace_source_handoff as handoff

    if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
        raise ValueError('Bybit handoff lock timeout must be finite and nonnegative')
    normalized = [Path(os.path.abspath(os.fspath(path))) for path in paths if path is not None]
    used_aliases = [alias for alias in ALIASES if any(path == alias or path.is_relative_to(alias)
                                                       for path in normalized)]
    source = bool(used_aliases) or _uses_root(normalized, SOURCE)
    destination = _uses_root(normalized, DESTINATION)
    if not source and not destination:
        yield
        return
    if handoff.BYBIT_NAME not in handoff.registered_names():
        if destination:
            raise SnapshotError('Bybit E staging generation is not an active writer workspace')
        yield
        return
    _validate_aliases(used_aliases)
    with _writer_guard(SOURCE, timeout_seconds):
        _validate_aliases(used_aliases)
        if destination:
            with handoff.selected_source(handoff.BYBIT_NAME):
                journal = handoff.read_journal()
                if (not journal or journal.get('state') != 'active'
                        or Path(journal['destination']) != DESTINATION
                        or not handoff.provider_workspace.exact_mount(SOURCE, DESTINATION)):
                    raise SnapshotError('Bybit E staging generation is not an active writer workspace')
        yield
