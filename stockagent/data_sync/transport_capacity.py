"""Complete no-follow capacity census with pinned, concurrent directory walks.

Unknown waves, failed copies and quarantined staging all consume capacity.
The caller retains the existing full-census retry and admission policy. Directory
and stat pools are separate so directory workers never starve their own child
I/O. Ancestor descriptors stay pinned until every descendant finishes.
"""
from __future__ import annotations
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import deque
from dataclasses import dataclass, field
import itertools
import os
from pathlib import Path
import stat
import threading

from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = "pinned_fd_frontier_capacity_v1"
WORKERS = 32
BATCH_REFERENCES = 128
MAX_OPEN_DIRECTORY_FRONTIER = 128


@dataclass(eq=False)
class _DirectoryFrame:
    parent: _DirectoryFrame | None
    name: str | None
    expected: os.stat_result
    is_staging: bool = False
    fd: int | None = None
    children: list[_DirectoryFrame] = field(default_factory=list)
    unfinished: int = 0
    total: int = 0
    staging: int = 0


def transport_usage(transport: Path) -> dict[str, int]:
    """Count all retained filename bytes, including unjournaled staging."""
    if any(p.is_symlink() for p in (transport, *transport.parents)):
        raise SnapshotError('transport capacity root is redirected')
    before = transport.stat()
    root_fd = os.open(transport, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    root = _DirectoryFrame(None, None, before, fd=root_fd)
    opened = {root}
    try:
        pinned = os.fstat(root_fd)
        if (pinned.st_dev, pinned.st_ino) != (before.st_dev, before.st_ino):
            raise SnapshotError('transport authority changed during capacity inventory')
        stat_slots = threading.BoundedSemaphore(BATCH_REFERENCES)
        with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix='transport-frontier-stat') as stats:
            with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix='transport-frontier-directory') as directories:
                def finish(node):
                    if node is root:
                        return
                    try:
                        after = os.stat(node.name, dir_fd=node.parent.fd, follow_symlinks=False)
                        if ((after.st_dev, after.st_ino) != (node.expected.st_dev, node.expected.st_ino)
                                or not stat.S_ISDIR(after.st_mode)):
                            raise SnapshotError('transport directory changed during its pinned walk')
                    finally:
                        os.close(node.fd)
                        node.fd = None

                def observe_batch(fd, names):
                    futures = []
                    try:
                        for name in names:
                            stat_slots.acquire()
                            try:
                                future = stats.submit(os.stat, name, dir_fd=fd, follow_symlinks=False)
                            except BaseException:
                                stat_slots.release()
                                raise
                            future.add_done_callback(lambda _: stat_slots.release())
                            futures.append(future)
                        return [f.result() for f in futures]
                    finally:
                        # Also on error, no descriptor can close before every
                        # submitted stat has stopped using it.
                        wait(futures)

                def scan(node):
                    if node is not root:
                        node.fd = os.open(node.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                          dir_fd=node.parent.fd)
                    try:
                        actual = os.fstat(node.fd)
                        if ((actual.st_dev, actual.st_ino) != (node.expected.st_dev, node.expected.st_ino)
                                or not stat.S_ISDIR(actual.st_mode)):
                            raise SnapshotError('transport directory changed before its pinned walk')
                        with os.scandir(node.fd) as iterator:
                            while names := [entry.name for entry in itertools.islice(iterator, BATCH_REFERENCES)]:
                                entries = observe_batch(node.fd, names)
                                for name, info in zip(names, entries, strict=True):
                                    stage = node.is_staging or node is root and name == '.staging'
                                    if info.st_dev != before.st_dev:
                                        raise SnapshotError('transport contains another mounted volume')
                                    if stat.S_ISREG(info.st_mode):
                                        node.total += info.st_size
                                        if stage:
                                            node.staging += info.st_size
                                    elif stat.S_ISDIR(info.st_mode):
                                        node.children.append(_DirectoryFrame(node, name, info, stage))
                                    else:
                                        raise SnapshotError('transport contains a redirected or nonregular file')
                        node.unfinished = len(node.children)
                        if not node.children:
                            finish(node)
                        return node
                    except BaseException:
                        if node is not root and node.fd is not None:
                            os.close(node.fd)
                            node.fd = None
                        raise

                pending = deque([('scan', root)])
                running = {}
                total = staging = 0

                def completed(node):
                    opened.discard(node)
                    if node.parent is not None:
                        node.parent.unfinished -= 1
                        if node.parent.unfinished == 0:
                            pending.appendleft(('finish', node.parent))

                try:
                    while pending or running:
                        while pending and len(running) < WORKERS:
                            kind, node = pending[0]
                            # DFS frontier closure bounds speculative directory
                            # FDs. A chain deeper than this window may borrow
                            # one FD at a time so the original depth is supported.
                            if (kind == 'scan' and len(opened) >= MAX_OPEN_DIRECTORY_FRONTIER
                                    and running):
                                break
                            pending.popleft()
                            if kind == 'scan':
                                opened.add(node)
                            running[directories.submit(scan if kind == 'scan' else finish, node)] = (kind, node)
                        done, _ = wait(running, return_when=FIRST_COMPLETED)
                        for future in done:
                            kind, node = running.pop(future)
                            future.result()
                            if kind == 'scan':
                                total += node.total
                                staging += node.staging
                                if node.children:
                                    pending.extendleft(('scan', child) for child in reversed(node.children))
                                else:
                                    completed(node)
                            else:
                                completed(node)
                finally:
                    # Running tasks can still reference ancestor descriptors.
                    # Join all tasks first, then close every surviving frame,
                    # including successful results after an earlier failure.
                    wait(running)
                    for node in opened:
                        if node is not root and node.fd is not None:
                            os.close(node.fd)
                            node.fd = None
        after = transport.stat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise SnapshotError('transport authority changed during capacity inventory')
        return {'retained_transport_bytes': total, 'retained_staging_bytes': staging}
    finally:
        os.close(root_fd)

