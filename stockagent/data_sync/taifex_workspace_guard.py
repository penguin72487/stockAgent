"""Fence TAIFEX public-history writers during the existing C-to-E handoff.

Unrelated/custom output roots remain independent. The shared lock allows the
public-history and rules collectors to run concurrently; the handoff owner
takes this same inode exclusively before it exchanges either namespace.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import sys

from stockagent.data_sync.yahoo_workspace_guard import _writer_guard

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT/'data_taifex_public_history'
WORKSPACE_NAME = 'taifex-public-history-e-primary-20261104-v1'
WORKSPACE_ROOT = Path('/srv/stockagent-workspace')


def _within(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


def _uses_source(output) -> bool:
    from stockagent.data_sync import workspace_volume

    path = Path(os.path.abspath(os.fspath(output)))
    source = Path(os.path.abspath(SOURCE))
    resolved = path.resolve(strict=False)
    if _within(path, source) or _within(resolved, source.resolve(strict=False)):
        return True
    # Also guard the exact E path if a caller bypasses the compatibility path,
    # even while the mount is unavailable and cannot be resolved on disk.
    if not (_within(path, WORKSPACE_ROOT) or _within(resolved, WORKSPACE_ROOT.resolve(strict=False))):
        return False
    config = workspace_volume.configuration()
    destination = Path(config['mount_point'])/'downloads'/WORKSPACE_NAME/source.name
    return _within(path, destination) or _within(resolved, destination.resolve(strict=False))


@contextmanager
def writer_guard(output, *, timeout_seconds: float = 30.0):
    if not _uses_source(output):
        yield
        return
    with _writer_guard(SOURCE, timeout_seconds):
        yield


@contextmanager
def cli_writer_guard(argv=None, *, default_output: str):
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument('--output-dir', default=default_output)
    args, _ = parser.parse_known_args(sys.argv[1:] if argv is None else argv)
    with writer_guard(args.output_dir):
        yield
