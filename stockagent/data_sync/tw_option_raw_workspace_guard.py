"""Fence official option raw writers while their C path moves to E.

The live settlement projections stay on C. Custom output directories are
unaffected, and an unactivated E staging path is never a writer workspace.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import math
import os
from pathlib import Path
import sys

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.yahoo_workspace_guard import _uses_root, _writer_guard

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT/'data_tw_index_options_daily/raw'
DESTINATION = (Path('/srv/stockagent-workspace/downloads') /
               'tw-index-options-raw-stream-e-20261108-v1/data_tw_index_options_daily/raw')


@contextmanager
def writer_guard(*paths, timeout_seconds: float = 30.0):
    from stockagent.data_sync import workspace_source_handoff as handoff

    if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
        raise ValueError('option raw handoff lock timeout must be finite and nonnegative')
    normalized = [Path(os.path.abspath(os.fspath(path))) for path in paths if path is not None]
    source = _uses_root(normalized, SOURCE)
    destination = _uses_root(normalized, DESTINATION)
    if not source and not destination:
        yield
        return
    if handoff.OPTIONS_RAW_NAME not in handoff.registered_names():
        if destination:
            raise SnapshotError('option raw E staging generation is not an active writer workspace')
        yield
        return
    with _writer_guard(SOURCE, timeout_seconds):
        if destination:
            with handoff.selected_source(handoff.OPTIONS_RAW_NAME):
                journal = handoff.read_journal()
                if (not journal or journal.get('state') != 'active'
                        or Path(journal['destination']) != DESTINATION
                        or not handoff.provider_workspace.exact_mount(SOURCE, DESTINATION)):
                    raise SnapshotError('option raw E staging generation is not an active writer workspace')
        yield


@contextmanager
def cli_writer_guard(argv=None):
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument('--output-dir', default='data_tw_index_options_daily')
    args, _ = parser.parse_known_args(sys.argv[1:] if argv is None else argv)
    with writer_guard(Path(args.output_dir).expanduser()/'raw'):
        yield
