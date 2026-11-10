"""Coordinate a bounded D-retirement turn without weakening storage owners."""
from contextlib import ExitStack, contextmanager
from pathlib import Path

from stockagent.data_sync import packed_snapshots
from stockagent.data_sync.desync_snapshots import SnapshotError

LOCK_NAME = 'source-local-retirement-turn.lock'
INTENT_LOCK_NAME = 'source-local-retirement-intent.lock'
PRESSURE_BYTES = 128 * 1024**3


def _root(root):
    root = Path(root).absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('retirement turn owner root is unavailable')
    return root


@contextmanager
def reservation(root: Path):
    """Prevent new D installers from entering while an existing one drains."""
    root = _root(root)
    with packed_snapshots._try_publication_lock(root/INTENT_LOCK_NAME):
        yield


@contextmanager
def turn(root: Path, *, retirement: bool):
    """Take a nonblocking turn; the original owners still authorize work."""
    root = _root(root)
    if retirement:
        with packed_snapshots._try_publication_lock(root/LOCK_NAME):
            yield
        return
    # Hold the shared admission gate only while acquiring the shared write
    # turn. The retirement owner can then close the gate while this already
    # admitted writer finishes, without interrupting or waiting on it here.
    with ExitStack() as writer:
        with packed_snapshots._try_publication_lock(root/INTENT_LOCK_NAME, shared=True):
            writer.enter_context(packed_snapshots._try_publication_lock(root/LOCK_NAME, shared=True))
        yield
