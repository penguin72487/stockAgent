#!/usr/bin/env python3
"""Continuously reconcile direct Syncthing artifacts with local-wins policy."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import select
import struct
import sys
import time
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.live_artifacts import (  # noqa: E402
    atomic_write_status,
    is_ignored_artifact,
    reconcile_artifacts,
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


class RecursiveInotify:
    _EVENT = struct.Struct("iIII")
    _IN_CLOSE_WRITE = 0x00000008
    _IN_ATTRIB = 0x00000004
    _IN_MOVED_FROM = 0x00000040
    _IN_MOVED_TO = 0x00000080
    _IN_CREATE = 0x00000100
    _IN_DELETE = 0x00000200
    _IN_DELETE_SELF = 0x00000400
    _IN_MOVE_SELF = 0x00000800
    _IN_Q_OVERFLOW = 0x00004000
    _IN_IGNORED = 0x00008000
    _IN_ISDIR = 0x40000000
    _WATCH_MASK = (
        _IN_CLOSE_WRITE
        | _IN_ATTRIB
        | _IN_MOVED_FROM
        | _IN_MOVED_TO
        | _IN_CREATE
        | _IN_DELETE
        | _IN_DELETE_SELF
        | _IN_MOVE_SELF
        | _IN_Q_OVERFLOW
    )

    def __init__(self, roots: dict[str, Path]) -> None:
        self.roots = {label: path.resolve() for label, path in roots.items()}
        self._fd: int | None = None
        self._watch: dict[int, tuple[str, Path]] = {}
        libc = ctypes.CDLL(None, use_errno=True)
        init = libc.inotify_init1
        init.argtypes = [ctypes.c_int]
        init.restype = ctypes.c_int
        fd = int(init(os.O_NONBLOCK | os.O_CLOEXEC))
        if fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1 failed")
        add_watch = libc.inotify_add_watch
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        self._libc = libc
        self._add_watch_fn = add_watch
        self._fd = fd
        for label, root in self.roots.items():
            self._add_tree(label, root)

    def _add_tree(self, label: str, root: Path) -> None:
        if self._fd is None:
            return
        for directory, dirnames, _filenames in os.walk(root, followlinks=False):
            path = Path(directory)
            relative = path.relative_to(self.roots[label])
            kept: list[str] = []
            for name in dirnames:
                child_relative = relative / name
                if (path / name).is_symlink():
                    continue
                if child_relative.parts and is_ignored_artifact(child_relative):
                    continue
                kept.append(name)
            dirnames[:] = kept
            descriptor = int(
                self._add_watch_fn(
                    self._fd,
                    os.fsencode(path),
                    self._WATCH_MASK,
                )
            )
            if descriptor >= 0:
                self._watch[descriptor] = (label, path)

    def wait(self, timeout: float) -> tuple[dict[str, set[PurePosixPath]], bool]:
        changes = {label: set() for label in self.roots}
        if self._fd is None:
            time.sleep(timeout)
            return changes, True
        ready, _writable, _errors = select.select([self._fd], [], [], timeout)
        if not ready:
            return changes, False
        overflow = False
        while True:
            try:
                data = os.read(self._fd, 1024 * 1024)
            except BlockingIOError:
                break
            if not data:
                break
            offset = 0
            while offset + self._EVENT.size <= len(data):
                descriptor, mask, _cookie, name_length = self._EVENT.unpack_from(
                    data, offset
                )
                offset += self._EVENT.size
                raw_name = data[offset : offset + name_length]
                offset += name_length
                if mask & self._IN_Q_OVERFLOW:
                    overflow = True
                    continue
                watched = self._watch.get(descriptor)
                if watched is None:
                    continue
                label, directory = watched
                if mask & self._IN_IGNORED:
                    self._watch.pop(descriptor, None)
                    continue
                name = os.fsdecode(raw_name.rstrip(b"\0"))
                path = directory / name if name else directory
                try:
                    relative = PurePosixPath(
                        path.relative_to(self.roots[label]).as_posix()
                    )
                except ValueError:
                    continue
                if not relative.parts or is_ignored_artifact(relative):
                    continue
                changes[label].add(relative)
                if mask & self._IN_ISDIR and mask & (
                    self._IN_CREATE | self._IN_MOVED_TO
                ):
                    self._add_tree(label, path)
        return changes, overflow

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
            self._watch.clear()


def _collapse_paths(paths: set[PurePosixPath]) -> list[PurePosixPath]:
    selected: list[PurePosixPath] = []
    for path in sorted(paths, key=lambda item: (len(item.parts), item.as_posix())):
        if any(parent == path or parent in path.parents for parent in selected):
            continue
        selected.append(path)
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-root", type=Path, default=Path("artifacts"))
    parser.add_argument(
        "--sync-root", type=Path, default=Path("/srv/stockagent-artifacts-hot")
    )
    parser.add_argument(
        "--status-path",
        type=Path,
        default=Path("/var/lib/stockagent-hot-artifact-sync/status.json"),
    )
    parser.add_argument("--settle-seconds", type=float, default=1.0)
    parser.add_argument("--full-scan-seconds", type=float, default=300.0)
    parser.add_argument("--once", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    local_root = args.local_root.resolve()
    sync_root = args.sync_root.resolve()
    started = time.monotonic()
    initial = reconcile_artifacts(local_root, sync_root)
    payload: dict[str, object] = {
        "state": "ready",
        "policy": "penguin-local-wins-no-delete",
        "local_root": str(local_root),
        "sync_root": str(sync_root),
        "last_reconcile_at": _utc_now(),
        "last_reconcile": initial.as_dict(),
        "startup_seconds": time.monotonic() - started,
    }
    atomic_write_status(args.status_path, payload)
    print(json.dumps(payload, sort_keys=True), flush=True)
    if args.once:
        return 0

    watcher = RecursiveInotify({"local": local_root, "sync": sync_root})
    last_full_scan = time.monotonic()
    try:
        while True:
            changes, overflow = watcher.wait(max(0.1, args.settle_seconds))
            now = time.monotonic()
            full_scan = overflow or now - last_full_scan >= args.full_scan_seconds
            aggregate: dict[str, int] = {}
            if full_scan:
                result = reconcile_artifacts(local_root, sync_root)
                aggregate = result.as_dict()
                last_full_scan = now
            else:
                paths = _collapse_paths(changes["local"] | changes["sync"])
                for relative in paths:
                    result = reconcile_artifacts(
                        local_root, sync_root, relative=relative
                    )
                    for key, value in result.as_dict().items():
                        aggregate[key] = aggregate.get(key, 0) + value
                for relative in _collapse_paths(changes["local"]):
                    local_path = local_root.joinpath(*relative.parts)
                    if not local_path.is_file() or local_path.is_symlink():
                        continue
                    result = reconcile_artifacts(
                        local_root,
                        sync_root,
                        relative=relative,
                        force_local_publish=True,
                    )
                    for key, value in result.as_dict().items():
                        aggregate[key] = aggregate.get(key, 0) + value
            if not aggregate and not full_scan:
                continue
            payload.update(
                {
                    "state": "ready",
                    "last_reconcile_at": _utc_now(),
                    "last_reconcile": aggregate,
                    "inotify_overflow": overflow,
                    "full_scan": full_scan,
                }
            )
            atomic_write_status(args.status_path, payload)
            if any(aggregate.values()):
                print(json.dumps(payload, sort_keys=True), flush=True)
    finally:
        watcher.close()


if __name__ == "__main__":
    raise SystemExit(main())
