from __future__ import annotations

import contextlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import dataclasses
import fcntl
import gzip
import hashlib
import io
import itertools
import json
import os
import re
import shutil
import stat
import tempfile
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping

from stockagent.data_sync.packed_metadata import DEFAULT_METADATA_MEMORY_ROWS, MetadataRows

from stockagent.data_sync.desync_snapshots import (
    DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    HLC,
    ResolvedSnapshot,
    SnapshotError,
    _contains_git_metadata,
    _exclusive_lock,
    _fsync_directory,
    _git_state,
    _load_json,
    _path_under,
    _paths_overlap,
    _safe_relative_path,
    _utc_iso_from_ns,
    atomic_write_bytes,
    atomic_write_json,
    default_node_id,
    next_hlc,
    scan_tree,
    sha256_file,
    validate_slug,
    write_immutable_json,
)


PACKED_SNAPSHOT_SCHEMA_VERSION = 1
PACKED_HEAD_SCHEMA_VERSION = 1
DEFAULT_LOOSE_FILE_THRESHOLD_BYTES = 8 * 1024 * 1024
DEFAULT_PACK_BUCKETS = 64
DEFAULT_COMPRESSION_LEVEL = 6
MAX_INCLUDED_ROOT_FILES = 4096
MAX_PACKED_PIN_BYTES = 16 * 1024 * 1024
MAX_IN_MEMORY_PACK_VERIFY_BYTES = 64 * 1024 * 1024
MAX_PACK_MEMBERS = 32768
PACK_WRITE_BUFFER_BYTES = 8 * 1024 * 1024
METADATA_MEMORY_ROWS = DEFAULT_METADATA_MEMORY_ROWS
MATERIALIZE_FSYNC_WORKERS = 4
MIN_PARALLEL_FSYNC_FILES = 128
PACKED_ARCHIVE_FORMAT = "stockagent-path-bucket-zip-v1"
CHUNKED_SNAPSHOT_SCHEMA_VERSION = 2
CHUNKED_ARCHIVE_FORMAT = "stockagent-chunked-path-bucket-zip-v2"
DEFAULT_FILE_CHUNK_BYTES = 8 * 1024 * 1024
MAX_FILE_CHUNK_BYTES = 64 * 1024 * 1024
MIN_NATIVE_CHUNK_STREAM_BYTES = 64 * 1024 * 1024
INVENTORY_FORMAT = "jsonl-gzip-v1"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_STAGING_RULES = frozenset({"artifact_io_atomic_sibling_v1", "finmind_parquet_promote_v1",
                                    "openbb_l1_atomic_sibling_v1", "openbb_operational_atomic_sibling_v1"})


class PackedPublicationBusy(SnapshotError):
    """An existing publisher owns this transaction; retry without blocking peers."""


@contextlib.contextmanager
def _try_publication_lock(path: Path, *, shared: bool = False, blocking: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise SnapshotError("publication lock is not regular")
        try:
            operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            fcntl.flock(descriptor, operation | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            raise PackedPublicationBusy(f"publication owner busy: {path.name}") from None
        if not os.path.samestat(opened, path.lstat()) or path.is_symlink():
            raise SnapshotError("publication lock changed during acquisition")
        yield
    finally:
        os.close(descriptor)


_PACK_STORED_SUFFIXES = {
    ".7z",
    ".bz2",
    ".feather",
    ".gif",
    ".gz",
    ".jpeg",
    ".jpg",
    ".lz4",
    ".npy",
    ".npz",
    ".orc",
    ".parquet",
    ".png",
    ".tar",
    ".webp",
    ".xz",
    ".zip",
    ".zst",
}
_STIGNORE = """// Node-local transactional state must never be replicated.
(?d).local-state
(?d).local-state/**
(?d).stignore-edge
#include .stignore-edge
"""
_EMPTY_EDGE_STIGNORE = """// Full-replica mode: no packed payload objects are ignored.
"""


@dataclasses.dataclass(slots=True)
class _SourceEntry:
    path: str
    kind: str
    mode: int
    size: int = 0
    target: str | None = None
    source_stat: tuple[int, int, int, int, int] | None = None
    sha256: str | None = None
    storage: dict[str, Any] | None = None
    # A private observation cohort may freeze a small mutable control file (or
    # use SQLite's backup API). Its logical name and mode remain unchanged.
    # Never serialized into a portable inventory.
    read_path: Path | None = None
    atomic_json_control: bool = False
    census_order: int | None = None

    def inventory_row(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "kind": self.kind,
            "mode": self.mode,
            "path": self.path,
        }
        if self.kind == "file":
            row.update(
                {
                    "sha256": self.sha256,
                    "size": self.size,
                    "storage": self.storage,
                }
            )
        elif self.kind == "symlink":
            row["target"] = self.target
        return row


def _entry_record(entry: _SourceEntry) -> dict:
    return {**entry.inventory_row(), "size": entry.size,
            "source_stat": entry.source_stat,
            **({'atomic_json_control': True} if entry.atomic_json_control else {}),
            **({'census_order': entry.census_order} if entry.census_order is not None else {}),
            "read_path": str(entry.read_path) if entry.read_path is not None else None}


def _entry_from_record(row: dict) -> _SourceEntry:
    return _SourceEntry(path=row["path"], kind=row["kind"], mode=row["mode"],
                        size=row.get("size", 0), target=row.get("target"),
                        source_stat=tuple(row["source_stat"]) if row.get("source_stat") else None,
                        sha256=row.get("sha256"), storage=row.get("storage"),
                        atomic_json_control=row.get("atomic_json_control", False),
                        census_order=row.get('census_order'),
                        read_path=Path(row["read_path"]) if row.get("read_path") is not None else None)


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _validate_hash(value: Any, label: str) -> str:
    text = str(value)
    if not _HASH_RE.fullmatch(text):
        raise SnapshotError(f"{label} must be a lowercase SHA-256; got {value!r}")
    return text


def _storage_extents(storage: Mapping[str, Any], size: int) -> list[tuple[str, int]]:
    """Ordered object references; repeated chunks count as repeated extents.

    The manifest still describes a complete tree, not a chain of deltas. This
    helper is also used by subset restore and object accounting, so no caller
    accidentally treats one chunk as the checksum of the complete file.
    """
    if storage.get("kind") != "chunks":
        return [(_validate_hash(storage.get("object_sha256"), "storage SHA-256"), size)]
    chunks = storage.get("chunks")
    if not isinstance(chunks, list) or not chunks:
        raise SnapshotError("chunked storage requires a nonempty ordered chunk list")
    result = []
    for chunk in chunks:
        if not isinstance(chunk, Mapping) or type(chunk.get("bytes")) is not int:
            raise SnapshotError("invalid chunk descriptor")
        count = chunk["bytes"]
        if not 1 <= count <= MAX_FILE_CHUNK_BYTES:
            raise SnapshotError("chunk size exceeds its bounded storage contract")
        result.append((_validate_hash(chunk.get("object_sha256"), "chunk SHA-256"), count))
    if sum(count for _, count in result) != size:
        raise SnapshotError("chunk extents differ from the complete file size")
    return result


def _source_stat(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        int(info.st_dev),
        int(info.st_ino),
        int(info.st_size),
        int(info.st_mtime_ns),
        int(info.st_ctime_ns),
    )


def _ensure_source_stat(path: Path, expected: _SourceEntry) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise SnapshotError(f"source entry disappeared while packing: {path}") from exc
    if not stat.S_ISREG(info.st_mode) or _source_stat(info) != expected.source_stat:
        raise SnapshotError(f"source file changed while packing: {path}")


@contextlib.contextmanager
def _open_source_entry(source: Path, entry: _SourceEntry):
    """Fuse the first path lookup with open, without trusting a stale census.

    The actual no-follow descriptor must match every original stat field.
    Recheck the path after reading so atomic replacement is still rejected.
    NONBLOCK makes a FIFO substituted after census reject instead of hanging.
    """
    path = _entry_read_path(source, entry)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    except OSError as error:
        raise SnapshotError(f"source entry disappeared or redirected while packing: {path}") from error
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or _source_stat(opened) != entry.source_stat:
            raise SnapshotError(f"source file changed while packing: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield stream
    finally:
        os.close(descriptor)
    _ensure_source_stat(path, entry)


def _validate_symlink_target(path: str, target: str) -> None:
    target_path = PurePosixPath(target)
    if target_path.is_absolute():
        raise SnapshotError(
            f"absolute symlink is not snapshot-safe: {path} -> {target}"
        )
    depth = len(PurePosixPath(path).parent.parts)
    for part in target_path.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            depth -= 1
            if depth < 0:
                raise SnapshotError(
                    f"symlink escapes the snapshot root: {path} -> {target}"
                )
        else:
            depth += 1


def _normalize_excluded_subtrees(values: Iterable[str]) -> tuple[str, ...]:
    selected: list[str] = []
    for value in sorted(set(values)):
        relative = _safe_relative_path(value, "excluded subtree").as_posix()
        if any(
            relative == parent or relative.startswith(f"{parent}/")
            for parent in selected
        ):
            continue
        selected.append(relative)
    return tuple(selected)


def _is_excluded(relative: str, excluded_subtrees: tuple[str, ...]) -> bool:
    return any(
        relative == prefix or relative.startswith(f"{prefix}/")
        for prefix in excluded_subtrees
    )


def _normalize_included_root_files(values: Iterable[str] | None) -> tuple[str, ...] | None:
    """An explicit, bounded set of root regular files, never a tree/glob.

    Per-key cache publication must not copy a staging tree or traverse unrelated
    generations. Keep this opt-in selection distinct from a full source census.
    """
    if values is None:
        return None
    if isinstance(values, (str, bytes, Mapping)):
        raise SnapshotError("included root files must be an explicit nonempty collection")
    try:
        iterator = iter(values)
    except TypeError as error:
        raise SnapshotError("included root files must be an explicit nonempty collection") from error
    selected = set()
    for index, name in enumerate(iterator):
        if index >= MAX_INCLUDED_ROOT_FILES:
            raise SnapshotError("included root file selection exceeds its bound")
        if (not isinstance(name, str) or name in {"", ".", "..", ".git"}
                or PurePosixPath(name).name != name or "\\" in name
                or any(ord(char) < 32 or ord(char) == 127 for char in name)):
            raise SnapshotError(f"unsafe included root file: {name!r}")
        if name in selected:
            raise SnapshotError(f"duplicate included root file: {name!r}")
        selected.add(name)
    if not selected:
        raise SnapshotError("included root files must not be empty")
    return tuple(sorted(selected))


def _normalize_private_staging_rules(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, Mapping)):
        raise SnapshotError("private staging rules must be an explicit collection")
    try:
        iterator = iter(values)
    except TypeError as error:
        raise SnapshotError("private staging rules must be an explicit collection") from error
    selected = []
    for index, value in enumerate(iterator):
        if (index >= len(_PRIVATE_STAGING_RULES) or not isinstance(value, str)
                or value not in _PRIVATE_STAGING_RULES):
            raise SnapshotError("unknown or excessive private writer staging rules")
        selected.append(value)
    return tuple(sorted(set(selected)))


def _is_private_writer_staging(relative: str, rules: tuple[str, ...]) -> bool:
    """Only canonical writers' uncommitted temporary names, never observations.

    artifact_io publishes its sibling after closing/fsyncing it. FinMind's
    complement writer then promotes latest.parquet to its SHA filename BEFORE
    publishing a receipt. Either transient name may normally disappear while a
    long private observation cohort is being read; no such name is a release.
    """
    parts = PurePosixPath(relative).parts
    if ("artifact_io_atomic_sibling_v1" in rules
            and re.fullmatch(r"\..+\.[0-9]+\.[0-9]+\.tmp", parts[-1])):
        return True
    if "openbb_l1_atomic_sibling_v1" in rules:
        # compact_openbb_l1 closes/validates these private siblings before
        # replace. Do not generalize to .tmp observations or other providers.
        match = re.fullmatch(r'\.(.+)\.[0-9]+\.tmp', parts[-1])
        if match and ((len(parts) == 1 and match[1] == 'openbb_l1.duckdb')
                      or (parts[0] == '_state' and match[1].endswith('.json'))
                      or (len(parts) == 2 and parts[0] == 'catalog'
                          and match[1] == 'l1_compaction_status.parquet')):
            return True
    if 'openbb_operational_atomic_sibling_v1' in rules:
        # The supervisor rotates via copy -> replace -> truncate. The monitor
        # closes catalog projections before replace. These exact temporaries
        # are not observations; migration freezes their reviewed final names.
        if relative == 'logs/supervisor.log.1.tmp':
            return True
        if (len(parts) == 2 and parts[0] == '_state'
                and re.fullmatch(r'archive_end_date\.txt\.tmp\.[0-9]+', parts[-1])):
            return True
        match = re.fullmatch(r'\.(.+)\.[0-9]+\.tmp', parts[-1])
        if (match and len(parts) == 2 and parts[0] == 'catalog'
                and match[1] in {'l1_compaction_status.parquet', 'l1_compaction_audit.parquet',
                    'provider_eta.parquet', 'category_eta.parquet', 'provider_endpoint_eta.parquet',
                    'coverage.parquet', 'equity_universe.parquet', 'currency_universe.parquet',
                    'configured_credentials.parquet', 'provider_rate_limits.parquet',
                    'completeness_contract_audit.parquet', 'compaction_summary.parquet',
                    'compaction_audit.parquet'}):
            return True
    if "finmind_parquet_promote_v1" in rules:
        if parts[0] in {"complement", "sponsor"}:
            parts = parts[1:]
        return (len(parts) == 5 and parts[0] == "parquet" and parts[-1] == "latest.parquet"
                and re.fullmatch(r"(?:[0-9a-f]{12}|all)", parts[2]) is not None)
    return False


def _collect_entries(
    source: Path,
    *,
    excluded_subtrees: tuple[str, ...] = (),
    maximum_file_bytes: int | None = None,
    included_root_files: tuple[str, ...] | None = None,
    file_overrides: Mapping[str, Path] | None = None,
    atomic_json_controls: bool = False,
    progress: Callable[[dict[str, Any]], None] | None = None,
    private_staging_file_rules: tuple[str, ...] = (),
    retain_entries: bool = True,
    entry_sink: MetadataRows[_SourceEntry] | None = None,
) -> tuple[list[_SourceEntry], dict[str, Any]]:
    """Census every admitted path; optionally retain its publication record.

    The final stability fence needs the same complete walk and fingerprints,
    not a second millions-of-record working set alongside the first inventory.
    """
    entries = entry_sink if entry_sink is not None else []
    portable = hashlib.sha256()
    stability = hashlib.sha256()
    files = directories = symlinks = logical_bytes = 0
    omitted_files = omitted_bytes = 0
    omitted_staging_files = 0
    used_overrides = set()
    last_progress = time.monotonic()
    if progress is not None:
        progress({"stage": "censusing_source", "files_completed": 0, "files_total": None,
                  "logical_bytes_processed": 0, "logical_bytes_total": None, "acceptance": False})

    def update_summary(
        relative: str, kind: str, info: os.stat_result, extra: str
    ) -> None:
        nonlocal files, directories, symlinks, logical_bytes, last_progress
        path_bytes = relative.encode("utf-8", errors="surrogateescape")
        extra_bytes = extra.encode("utf-8", errors="surrogateescape")
        portable_size = info.st_size if kind == "F" else 0
        portable.update(kind.encode("ascii") + b"\0" + path_bytes + b"\0")
        portable.update(
            str(portable_size).encode("ascii") + b"\0" + extra_bytes + b"\n"
        )
        stability.update(kind.encode("ascii") + b"\0" + path_bytes + b"\0")
        stability.update(
            (
                f"{info.st_size}:{info.st_mode}:{info.st_mtime_ns}:"
                f"{info.st_ctime_ns}:{info.st_dev}:{info.st_ino}:"
            ).encode("ascii")
            + extra_bytes
            + b"\n"
        )
        if kind == "F":
            files += 1
            logical_bytes += info.st_size
        elif kind == "D":
            directories += 1
        elif kind == "L":
            symlinks += 1
        if (progress is not None and (files + directories + symlinks) % 1024 == 0
                and time.monotonic() - last_progress >= 5):
            last_progress = time.monotonic()
            progress({"stage": "censusing_source", "files_completed": files, "files_total": None,
                      "logical_bytes_processed": logical_bytes, "logical_bytes_total": None,
                      "acceptance": False})

    if included_root_files is not None:
        root_before = source.lstat()
        if not stat.S_ISDIR(root_before.st_mode):
            raise SnapshotError("selected snapshot source must be a real directory")
        for name in included_root_files:
            path = source / name
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                raise SnapshotError(f"included root file is not regular: {path}")
            update_summary(name, "F", info, "")
            if retain_entries:
                entries.append(_SourceEntry(path=name, kind="file", mode=stat.S_IMODE(info.st_mode),
                                            size=int(info.st_size), source_stat=_source_stat(info),
                                            census_order=files + directories + symlinks - 1 if atomic_json_controls else None))
        root_after = source.lstat()
        if ((root_before.st_dev, root_before.st_ino, root_before.st_mode)
                != (root_after.st_dev, root_after.st_ino, root_after.st_mode)):
            raise SnapshotError("selected snapshot source directory changed during census")
        walks = ()
    else:
        def walk_error(error):
            raise error
        walks = os.walk(source, topdown=True, followlinks=False, onerror=walk_error)

    for current, dir_names, file_names in walks:
        dir_names.sort()
        file_names.sort()
        current_path = Path(current)
        for name in list(dir_names):
            path = current_path / name
            relative = path.relative_to(source).as_posix()
            if _is_excluded(relative, excluded_subtrees):
                dir_names.remove(name)
                continue
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                dir_names.remove(name)
                target = os.readlink(path)
                _validate_symlink_target(relative, target)
                update_summary(relative, "L", info, target)
                if retain_entries:
                    entries.append(
                        _SourceEntry(
                            path=relative,
                            kind="symlink",
                            mode=stat.S_IMODE(info.st_mode),
                            target=target,
                            census_order=files + directories + symlinks - 1 if atomic_json_controls else None,
                        )
                    )
            elif stat.S_ISDIR(info.st_mode):
                update_summary(relative, "D", info, "")
                if retain_entries:
                    entries.append(
                        _SourceEntry(
                            path=relative,
                            kind="directory",
                            mode=stat.S_IMODE(info.st_mode),
                            census_order=files + directories + symlinks - 1 if atomic_json_controls else None,
                        )
                    )
            else:
                raise SnapshotError(f"unsupported directory entry type: {path}")
        for name in file_names:
            path = current_path / name
            relative = path.relative_to(source).as_posix()
            if _is_excluded(relative, excluded_subtrees):
                continue
            if private_staging_file_rules and _is_private_writer_staging(relative, private_staging_file_rules):
                omitted_staging_files += 1
                continue
            info = path.lstat()
            if stat.S_ISREG(info.st_mode):
                override = (file_overrides or {}).get(relative)
                if override is not None:
                    used_overrides.add(relative)
                payload_info = override.lstat() if override is not None else info
                if not stat.S_ISREG(payload_info.st_mode):
                    raise SnapshotError(f"frozen control payload is not regular: {relative}")
                if maximum_file_bytes is not None and info.st_size > maximum_file_bytes:
                    omitted_files += 1
                    omitted_bytes += int(info.st_size)
                    continue
                update_summary(relative, "F", payload_info, "")
                if retain_entries:
                    entries.append(
                        _SourceEntry(
                            path=relative,
                            kind="file",
                            mode=stat.S_IMODE(info.st_mode),
                            size=int(payload_info.st_size),
                            source_stat=_source_stat(payload_info),
                            read_path=override,
                            atomic_json_control=(atomic_json_controls and override is None
                                                 and _is_atomic_json_control(source, relative)),
                            census_order=files + directories + symlinks - 1 if atomic_json_controls else None,
                        )
                    )
            elif stat.S_ISLNK(info.st_mode):
                target = os.readlink(path)
                _validate_symlink_target(relative, target)
                update_summary(relative, "L", info, target)
                if retain_entries:
                    entries.append(
                        _SourceEntry(
                            path=relative,
                            kind="symlink",
                            mode=stat.S_IMODE(info.st_mode),
                            target=target,
                            census_order=files + directories + symlinks - 1 if atomic_json_controls else None,
                        )
                    )
            else:
                raise SnapshotError(f"special files are not snapshot-safe: {path}")
    if used_overrides != set(file_overrides or {}):
        raise SnapshotError('frozen observation paths disappeared or were excluded before the cohort census')
    entries.sort(key=lambda entry: entry.path)
    return entries, {
        "files": files,
        "directories": directories,
        "symlinks": symlinks,
        "logical_bytes": logical_bytes,
        "omitted_files_above_maximum": omitted_files,
        "omitted_bytes_above_maximum": omitted_bytes,
        **({"omitted_uncommitted_staging_files": omitted_staging_files}
           if private_staging_file_rules else {}),
        "portable_fingerprint_sha256": portable.hexdigest(),
        "stability_fingerprint_sha256": stability.hexdigest(),
    }


def _entry_read_path(source: Path, entry: _SourceEntry) -> Path:
    return entry.read_path or source.joinpath(*PurePosixPath(entry.path).parts)


def _is_atomic_json_control(source: Path, logical: str) -> bool:
    """Exact existing producer receipt/state scopes, never raw JSON by basename."""
    if not logical.endswith('.json'):
        return False
    trees = {
        'data_finlab': ('receipts', 'raw_receipts', 'attempts', 'intraday/receipts', 'intraday/attempts',
                        'intraday/derived_minute/receipts'),
        'data_finmind': ('receipts', 'complement/receipts', 'sponsor/receipts'),
        'data_crypto_reference': ('state/providers', 'state/datasets'),
    }.get(source.name, ())
    if source.name == 'data_tej':
        return PurePosixPath(logical).parent.as_posix() == 'progress'
    return any(logical.startswith(root + '/') for root in trees)


def _read_atomic_json_control(source: Path, entry: _SourceEntry) -> bytes:
    """Observe one complete JSON from its stable FD, not a global-time snapshot.

    No additional file, omitted fsync or mutable-source hardlink is created.
    Cold ZIP installation and full independent reconstruction remain required.
    An atomic replacement after open cannot change the held observation; an
    in-place write must pass the bounded FD/size/JSON fences or be retried.
    """
    path = _entry_read_path(source, entry)
    if entry.read_path is not None or not _is_atomic_json_control(source, entry.path):
        raise SnapshotError('atomic JSON observation is outside its named source scope')
    for attempt in range(3):
        if path.parent.resolve() != path.parent:
            raise SnapshotError('atomic JSON control parent is redirected')
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        with os.fdopen(descriptor, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > 64 * 1024**2:
                raise SnapshotError('atomic JSON control exceeds its bounded regular-file scope')
            raw = stream.read(64 * 1024**2 + 1)
            after = os.fstat(stream.fileno())
        if _source_stat(before)[:4] == _source_stat(after)[:4] and len(raw) == before.st_size:
            try:
                json.loads(raw)
            except ValueError:
                if attempt == 2:
                    raise
                continue
            entry.size = len(raw)
            entry.mode = stat.S_IMODE(before.st_mode)
            entry.source_stat = _source_stat(before)
            return raw
        if attempt == 2:
            raise SnapshotError(f'atomic JSON control changed during bounded observation: {entry.path}')
    raise AssertionError('bounded JSON observation did not finish')


def _guard_observation_cohort(source: Path, entries: Iterable[_SourceEntry], root_identity) -> None:
    """Additions are the next version; changes to selected bytes still fail.

    This is not an atomic workspace/database snapshot and grants no shared
    publication or source-retirement authority. Every selected original file
    is fully hashed as usual. Pre-frozen controls/known published mutable
    outputs use read_path. Named JSON controls explicitly opt into stable-FD
    observations at read time; unknown selected payload mutations still fail.
    """
    if _source_stat(source.lstat())[:2] != root_identity:
        raise SnapshotError("observation cohort root was replaced")
    for entry in entries:
        path = _entry_read_path(source, entry)
        if entry.kind == "file":
            if entry.atomic_json_control:
                # These exact bytes were observed and completely hashed during
                # routing/packing, not promised to remain today's current head.
                if not entry.sha256 or not entry.storage:
                    raise SnapshotError('atomic JSON observation has no sealed storage')
            else:
                _ensure_source_stat(path, entry)
        elif entry.kind == "directory":
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != entry.mode:
                raise SnapshotError(f"selected cohort directory changed: {path}")
        elif not path.is_symlink() or os.readlink(path) != entry.target:
            raise SnapshotError(f"selected cohort symlink changed: {path}")


def _copy_and_hash(source: Path, destination: Path) -> str:
    digest = hashlib.sha256()
    with source.open("rb") as input_stream, destination.open("xb") as output_stream:
        while block := input_stream.read(8 * 1024 * 1024):
            digest.update(block)
            output_stream.write(block)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    return digest.hexdigest()


def _native_copy_and_hash(source: Path, destination: Path) -> str:
    """Durable C-to-D stream with independent Linux and Windows full SHA."""
    from stockagent.data_sync.windows_cold_io import BinaryWriter

    digest = hashlib.sha256()
    writer = BinaryWriter(destination)
    try:
        with source.open("rb") as stream:
            while block := stream.read(8 * 1024 * 1024):
                digest.update(block)
                writer.write(block)
        result = writer.finish()
        if result.get("sha256") != digest.hexdigest():
            raise SnapshotError("native durable copy checksum differs; partial retained")
        return digest.hexdigest()
    finally:
        writer.close()


def _install_immutable_object(
    sync_root: Path,
    temporary: Path,
    destination: Path,
    *,
    expected_sha256: str,
    pending_directories: set[Path] | None = None,
) -> bool:
    _ensure_shared_packed_directory(sync_root, destination.parent)
    reused = False
    if destination.exists():
        if destination.is_symlink() or sha256_file(destination) != expected_sha256:
            raise SnapshotError(f"content-addressed object is corrupt: {destination}")
        reused = True
    else:
        # Persist bytes BEFORE exposing the immutable name to another publisher.
        # Include ZIP payloads, not just already-fsynced blobs/inventories.
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        # Link never replaces a retained object's inode under a live reader.
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError:
            if destination.is_symlink() or sha256_file(destination) != expected_sha256:
                raise SnapshotError(f"content-addressed object is corrupt: {destination}") from None
            reused = True
    temporary.unlink()
    # Even a reused winner may belong to a concurrent publisher that has not
    # flushed its directories yet. Our head must not depend on its later fsync.
    if pending_directories is None:
        _fsync_directory(destination.parent)
    else:
        parent = destination.parent
        while parent != sync_root:
            pending_directories.add(parent)
            parent = parent.parent
        pending_directories.add(sync_root)
    return reused


def _flush_object_directories(directories: set[Path]) -> None:
    """Persist every rename before any manifest/head or transport notification.

    Multiple immutable objects in a shard share one directory fsync, never a
    weaker data verification. An error must block the publication, not hide it.
    """
    for directory in sorted(directories, key=lambda p: (-len(p.parts), str(p))):
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _bucket_for_path(path: str, bucket_count: int) -> int:
    digest = hashlib.sha256(path.encode("utf-8", errors="surrogateescape")).digest()
    return int.from_bytes(digest[:8], "big") % bucket_count


def _zip_info(entry: _SourceEntry) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(entry.path, date_time=(1980, 1, 1, 0, 0, 0))
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | entry.mode) << 16
    info.compress_type = (
        zipfile.ZIP_STORED
        if Path(entry.path).suffix.lower() in _PACK_STORED_SUFFIXES
        else zipfile.ZIP_DEFLATED
    )
    return info


class _SeekablePackBuffer:
    """Coalesce ZIP header backpatches without changing a single ZIP byte.

    BufferedWriter flushes on every seek. ZIP revisits each member's header,
    making millions of small cold-drive writes despite a large write buffer.
    Keep one bounded dirty window instead. Seeks within it only move a logical
    cursor; a seek outside it first drains all bytes to the original file.
    Large members retain ordinary seek/backpatch behavior. This is not a new
    archive encoding, a source cache, or a relaxed durability boundary.
    """

    def __init__(self, stream, *, capacity: int = PACK_WRITE_BUFFER_BYTES):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("pack write buffer must be positive")
        self._stream = stream
        self._capacity = capacity
        self._position = self._start = stream.tell()
        self._buffer = bytearray()

    def tell(self):
        return self._position

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            target = offset
        elif whence == os.SEEK_CUR:
            target = self._position + offset
        else:
            raise io.UnsupportedOperation("ZIP output only supports absolute/current seeks")
        if target < 0:
            raise ValueError("negative pack output position")
        if self._start <= target <= self._start + len(self._buffer):
            self._position = target
        else:
            self.flush()
            self._stream.seek(target)
            self._position = self._start = target
        return self._position

    def write(self, block):
        view = memoryview(block)
        length = len(view)
        while view:
            offset = self._position - self._start
            available = self._capacity - offset
            if not available:
                self.flush()
                continue
            count = min(available, len(view))
            self._buffer[offset:offset + count] = view[:count]
            self._position += count
            view = view[count:]
        return length

    def flush(self):
        if self._buffer:
            self._stream.seek(self._start)
            view = memoryview(self._buffer)
            try:
                while view:
                    written = self._stream.write(view)
                    if not isinstance(written, int) or not 0 < written <= len(view):
                        raise OSError("short pack write; retain uncommitted staging")
                    view = view[written:]
            finally:
                view.release()
            self._buffer.clear()
        self._start = self._position
        self._stream.flush()


def _write_pack(
    source: Path,
    entries: Iterable[_SourceEntry],
    temporary: Path,
    *,
    compression_level: int,
    on_entry: Callable[[_SourceEntry], None] | None = None,
) -> None:
    with temporary.open("xb", buffering=0) as stream, zipfile.ZipFile(
        _SeekablePackBuffer(stream),
        mode="x",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=compression_level,
        allowZip64=True,
    ) as archive:
        for entry in entries:
            source_path = _entry_read_path(source, entry)
            if entry.atomic_json_control:
                raw = _read_atomic_json_control(source, entry)
                with archive.open(_zip_info(entry), mode='w', force_zip64=True) as output_stream:
                    output_stream.write(raw)
                entry.sha256 = hashlib.sha256(raw).hexdigest()
                if on_entry is not None:
                    on_entry(entry)
                continue
            digest = hashlib.sha256()
            with (
                _open_source_entry(source, entry) as input_stream,
                archive.open(
                    _zip_info(entry), mode="w", force_zip64=True
                ) as output_stream,
            ):
                while block := input_stream.read(8 * 1024 * 1024):
                    digest.update(block)
                    output_stream.write(block)
            if entry.sha256 is not None and entry.sha256 != digest.hexdigest():
                raise SnapshotError(f"source file changed while packing: {source_path}")
            entry.sha256 = digest.hexdigest()
            if on_entry is not None:
                on_entry(entry)


def _hash_source_entry(source: Path, entry: _SourceEntry, *, native_d_io: bool = False) -> str:
    if entry.atomic_json_control:
        return hashlib.sha256(_read_atomic_json_control(source, entry)).hexdigest()
    source_path = _entry_read_path(source, entry)
    if native_d_io:
        _ensure_source_stat(source_path, entry)
        from stockagent.data_sync.windows_cold_io import hash_file
        digest = hash_file(source_path)
        _ensure_source_stat(source_path, entry)
    else:
        value = hashlib.sha256()
        with _open_source_entry(source, entry) as stream:
            while block := stream.read(8 * 1024 * 1024):
                value.update(block)
        digest = value.hexdigest()
    return digest


def _is_ordered_subset(selected: list[str], available: list[str]) -> bool:
    iterator = iter(available)
    return all(any(candidate == expected for candidate in iterator) for expected in selected)


def _write_inventory(temporary: Path, entries: Iterable[_SourceEntry]) -> str:
    digest = hashlib.sha256()
    with temporary.open("xb") as raw_stream:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=6,
            fileobj=raw_stream,
            mtime=0,
        ) as stream:
            for entry in entries:
                stream.write(_canonical_json_bytes(entry.inventory_row()))
        raw_stream.flush()
        os.fsync(raw_stream.fileno())
    with temporary.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _object_relpath(kind: str, digest: str, suffix: str) -> PurePosixPath:
    return PurePosixPath("objects") / kind / digest[:2] / f"{digest}{suffix}"


def _ensure_shared_packed_directory(sync_root: Path, directory: Path) -> None:
    """Create one public packed directory with cooperative writer permissions.

    Most nodes run the publisher and Syncthing as the same Unix user. Vast
    containers deliberately run Syncthing as ``user`` while root-owned cron
    publishes releases. When the packed root explicitly opts into group-write,
    retain that policy and setgid inheritance below ``heads``, ``manifests``,
    and ``objects``. A normal 0755 single-user root is left unchanged.
    """

    sync_root = sync_root.resolve()
    directory = directory.resolve(strict=False)
    try:
        relative = directory.relative_to(sync_root)
    except ValueError as exc:
        raise SnapshotError(
            f"packed directory escapes sync root: {directory}"
        ) from exc
    if not relative.parts or relative.parts[0] not in {
        "heads",
        "manifests",
        "objects",
    }:
        directory.mkdir(parents=True, exist_ok=True)
        return
    directory.mkdir(parents=True, exist_ok=True)
    root_mode = stat.S_IMODE(sync_root.stat().st_mode)
    if not root_mode & stat.S_IWGRP:
        return
    inherited = stat.S_IWGRP | stat.S_IXGRP
    if root_mode & stat.S_ISGID:
        inherited |= stat.S_ISGID
    current = sync_root
    for component in relative.parts:
        current /= component
        info = current.stat(follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode):
            raise SnapshotError(f"packed path component is not a directory: {current}")
        mode = stat.S_IMODE(info.st_mode)
        if mode | inherited != mode:
            current.chmod(mode | inherited)


def repair_shared_packed_directory_modes(sync_root: Path) -> int:
    """Repair public directory modes for a group-writable packed root."""

    sync_root = sync_root.resolve()
    if not sync_root.exists():
        return 0
    root_mode = stat.S_IMODE(sync_root.stat().st_mode)
    if not root_mode & stat.S_IWGRP:
        return 0
    changed = 0
    inherited = stat.S_IWGRP | stat.S_IXGRP
    if root_mode & stat.S_ISGID:
        inherited |= stat.S_ISGID
    for top in ("heads", "manifests", "objects"):
        public_root = sync_root / top
        if not public_root.exists():
            continue
        for directory, dirnames, _filenames in os.walk(public_root, followlinks=False):
            dirnames.sort()
            path = Path(directory)
            info = path.stat(follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                continue
            mode = stat.S_IMODE(info.st_mode)
            if mode | inherited != mode:
                path.chmod(mode | inherited)
                changed += 1
    return changed


def initialize_packed_layout(
    sync_root: Path,
    *,
    node_id: str | None = None,
    replace_ignore: bool = False,
    replace_node_id: bool = False,
) -> str:
    sync_root = sync_root.resolve()
    if (sync_root / ".stockagent-d-mount-required").exists():
        raise SnapshotError(
            f"D-backed cold volume is not mounted at {sync_root}; "
            "refusing to recreate the packed store on the fallback filesystem"
        )
    if git_root := _contains_git_metadata(sync_root):
        raise SnapshotError(
            f"sync root {sync_root} is inside Git worktree {git_root}; "
            "use a separate data-only directory"
        )
    resolved_node_id = validate_slug(node_id or default_node_id(), "node_id")
    for relative in (
        "heads",
        "manifests",
        "objects/blobs",
        "objects/packs",
        "objects/inventories",
    ):
        _ensure_shared_packed_directory(sync_root, sync_root / relative)
    for relative in (".local-state/locks", ".local-state/staging"):
        (sync_root / relative).mkdir(parents=True, exist_ok=True)
    repair_shared_packed_directory_modes(sync_root)
    ignore_path = sync_root / ".stignore"
    edge_ignore_path = sync_root / ".stignore-edge"
    if not edge_ignore_path.exists():
        atomic_write_bytes(edge_ignore_path, _EMPTY_EDGE_STIGNORE.encode("utf-8"))
    if replace_ignore or not ignore_path.exists():
        atomic_write_bytes(ignore_path, _STIGNORE.encode("utf-8"))
    node_path = sync_root / ".local-state" / "node-id"
    if node_path.exists():
        existing = validate_slug(
            node_path.read_text(encoding="utf-8").strip(), "stored node_id"
        )
        if node_id is None:
            return existing
        if existing != resolved_node_id and not replace_node_id:
            raise SnapshotError(
                f"node_id is already {existing}; refusing to change it to "
                f"{resolved_node_id} without --replace-node-id"
            )
    atomic_write_bytes(node_path, f"{resolved_node_id}\n".encode(), mode=0o600)
    return resolved_node_id


def _resolve_node_id(sync_root: Path, explicit: str | None = None) -> str:
    requested = validate_slug(explicit, "node_id") if explicit else None
    if from_env := os.environ.get("STOCKAGENT_SYNC_NODE_ID"):
        env_node = validate_slug(from_env, "STOCKAGENT_SYNC_NODE_ID")
        if requested is not None and requested != env_node:
            raise SnapshotError(
                f"--node-id {requested} disagrees with STOCKAGENT_SYNC_NODE_ID "
                f"{env_node}"
            )
        requested = env_node
    node_path = sync_root / ".local-state" / "node-id"
    try:
        stored = validate_slug(
            node_path.read_text(encoding="utf-8").strip(), "stored node_id"
        )
    except OSError as exc:
        raise SnapshotError(
            f"packed sync root is not initialized: {sync_root}"
        ) from exc
    if requested is not None and requested != stored:
        raise SnapshotError(
            f"requested node_id {requested} disagrees with initialized node_id {stored}"
        )
    return stored


def _validate_manifest(manifest: Mapping[str, Any], *,
                       object_validation_cache: set[tuple[str, str, str, int]] | None = None) -> HLC:
    schema = int(manifest.get("schema_version", -1))
    if schema not in {PACKED_SNAPSHOT_SCHEMA_VERSION, CHUNKED_SNAPSHOT_SCHEMA_VERSION}:
        raise SnapshotError("unsupported packed snapshot manifest schema")
    dataset = validate_slug(str(manifest.get("dataset", "")), "manifest dataset")
    snapshot_id = validate_slug(
        str(manifest.get("snapshot_id", "")), "manifest snapshot_id"
    )
    publisher = manifest.get("publisher")
    archive = manifest.get("archive")
    source = manifest.get("source")
    if not all(isinstance(value, Mapping) for value in (publisher, archive, source)):
        raise SnapshotError(f"manifest {snapshot_id} is incomplete")
    expected_format = (CHUNKED_ARCHIVE_FORMAT if schema == CHUNKED_SNAPSHOT_SCHEMA_VERSION
                       else PACKED_ARCHIVE_FORMAT)
    if archive.get("format") != expected_format:
        raise SnapshotError(f"manifest {snapshot_id} has unsupported archive format")
    if schema == CHUNKED_SNAPSHOT_SCHEMA_VERSION:
        count = archive.get("file_chunk_bytes")
        if type(count) is not int or not 1 <= count <= MAX_FILE_CHUNK_BYTES:
            raise SnapshotError(f"manifest {snapshot_id} has invalid chunk policy")
    publisher_node = validate_slug(
        str(publisher.get("node_id", "")), "publisher node_id"
    )
    stamp = HLC.from_mapping(manifest.get("hlc", {}))
    if stamp.node_id != publisher_node:
        raise SnapshotError(f"manifest {snapshot_id} has inconsistent publisher HLC")
    inventory = archive.get("inventory")
    objects = archive.get("objects")
    if not isinstance(inventory, Mapping) or not isinstance(objects, list):
        raise SnapshotError(f"manifest {snapshot_id} has invalid inventory/objects")
    _safe_relative_path(str(inventory.get("relpath", "")), "inventory relpath")
    _validate_hash(inventory.get("sha256"), "inventory SHA-256")
    seen_hashes: set[str] = set()
    for item in objects:
        if not isinstance(item, Mapping):
            raise SnapshotError(f"manifest {snapshot_id} contains an invalid object")
        cache_key = None
        if (object_validation_cache is not None
                and type(item.get("sha256")) is str and type(item.get("relpath")) is str
                and type(item.get("kind")) is str and type(item.get("bytes")) is int):
            cache_key = (item["sha256"], item["relpath"], item["kind"], item["bytes"])
        if cache_key is not None and cache_key in object_validation_cache:
            digest = item["sha256"]
        else:
            digest = _validate_hash(item.get("sha256"), "object SHA-256")
            _safe_relative_path(str(item.get("relpath", "")), "object relpath")
            if item.get("kind") not in {"blob", "pack"}:
                raise SnapshotError(f"manifest {snapshot_id} has invalid object kind")
            if type(item.get("bytes")) is not int or item["bytes"] < 0:
                raise SnapshotError(f"manifest {snapshot_id} has invalid object size")
            if cache_key is not None:
                object_validation_cache.add(cache_key)
        if digest in seen_hashes:
            raise SnapshotError(f"manifest {snapshot_id} repeats object {digest}")
        seen_hashes.add(digest)
    fingerprint = str(source.get("portable_fingerprint_sha256", ""))
    _validate_hash(fingerprint, "source fingerprint")
    if not dataset:
        raise SnapshotError("manifest dataset is empty")
    return stamp


def _validate_object_presence(sync_root: Path, manifest: Mapping[str, Any]) -> None:
    archive = manifest["archive"]
    references = [archive["inventory"], *archive["objects"]]
    for item in references:
        path = _path_under(sync_root, str(item["relpath"]), "object relpath")
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise SnapshotError(
                f"snapshot object is not locally complete: {path}"
            ) from exc
        if size != int(item["bytes"]):
            raise SnapshotError(
                f"snapshot object size mismatch for {path}: "
                f"expected {item['bytes']}, got {size}"
            )


def _head_candidates(
    sync_root: Path,
    dataset: str,
    *,
    now_ns: int,
    max_clock_skew_seconds: int,
    require_objects: bool = True,
) -> tuple[list[ResolvedSnapshot], list[str], list[tuple[HLC | None, str]]]:
    candidates: list[ResolvedSnapshot] = []
    diagnostics: list[str] = []
    incomplete: list[tuple[HLC | None, str]] = []
    future_limit_ns = int(max_clock_skew_seconds) * 1_000_000_000
    for head_path in sorted((sync_root / "heads" / dataset).glob("*.json")):
        head_stamp: HLC | None = None
        canonical = False
        try:
            head = _load_json(head_path)
            if int(head.get("schema_version", -1)) != PACKED_HEAD_SCHEMA_VERSION:
                raise SnapshotError("unsupported packed head schema")
            if validate_slug(str(head.get("dataset", "")), "head dataset") != dataset:
                raise SnapshotError("head dataset differs from requested dataset")
            node_id = validate_slug(str(head.get("node_id", "")), "head node_id")
            canonical = head_path.name == f"{node_id}.json"
            if not canonical:
                raise SnapshotError("head filename does not match its node_id")
            head_stamp = HLC.from_mapping(head.get("hlc", {}))
            if head_stamp.physical_ns - now_ns > future_limit_ns:
                raise SnapshotError(
                    f"head from {node_id} is in the future; repair clock synchronization"
                )
            manifest_path = _path_under(
                sync_root, str(head.get("manifest_relpath", "")), "manifest relpath"
            )
            manifest_sha = _validate_hash(
                head.get("manifest_sha256"), "manifest SHA-256"
            )
            if sha256_file(manifest_path) != manifest_sha:
                raise SnapshotError("manifest checksum mismatch")
            manifest = _load_json(manifest_path)
            manifest_stamp = _validate_manifest(manifest)
            if manifest_stamp != head_stamp:
                raise SnapshotError("head and manifest HLC differ")
            if manifest.get("snapshot_id") != head.get("snapshot_id"):
                raise SnapshotError("head and manifest snapshot IDs differ")
            if require_objects:
                _validate_object_presence(sync_root, manifest)
            candidates.append(
                ResolvedSnapshot(
                    manifest=manifest,
                    manifest_path=manifest_path,
                    manifest_sha256=manifest_sha,
                    head_path=head_path,
                )
            )
        except (OSError, SnapshotError, TypeError, ValueError) as exc:
            message = f"{head_path.name}: {exc}"
            diagnostics.append(message)
            if canonical or (
                head_stamp is None and ".sync-conflict-" not in head_path.name
            ):
                incomplete.append((head_stamp, message))
    return candidates, diagnostics, incomplete


def resolve_latest_packed(
    sync_root: Path,
    dataset: str,
    *,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    now_ns: int | None = None,
    require_objects: bool = True,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    dataset = validate_slug(dataset, "dataset")
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    candidates, diagnostics, incomplete = _head_candidates(
        sync_root,
        dataset,
        now_ns=current_ns,
        max_clock_skew_seconds=max_clock_skew_seconds,
        require_objects=require_objects,
    )
    if not candidates:
        detail = "; ".join(diagnostics) if diagnostics else "no per-node heads found"
        raise SnapshotError(f"no valid packed snapshot for {dataset}: {detail}")
    winner = max(
        candidates,
        key=lambda item: (
            HLC.from_mapping(item.manifest["hlc"]),
            str(item.manifest["snapshot_id"]),
        ),
    )
    winner_stamp = HLC.from_mapping(winner.manifest["hlc"])
    blocking = [
        message
        for stamp, message in incomplete
        if stamp is None or stamp >= winner_stamp
    ]
    if blocking:
        raise SnapshotError(
            "newest canonical packed head is not locally complete; refusing to fall "
            f"back to {winner.manifest['snapshot_id']}: {'; '.join(blocking)}"
        )
    return dataclasses.replace(winner, diagnostics=tuple(diagnostics))


def resolve_packed_snapshot_id(
    sync_root: Path,
    dataset: str,
    snapshot_id: str,
    *,
    require_objects: bool = True,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    dataset = validate_slug(dataset, "dataset")
    snapshot_id = validate_slug(snapshot_id, "snapshot_id")
    manifest_path = sync_root / "manifests" / dataset / f"{snapshot_id}.json"
    manifest_sha = sha256_file(manifest_path)
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    if manifest.get("dataset") != dataset or manifest.get("snapshot_id") != snapshot_id:
        raise SnapshotError(f"manifest identity mismatch: {manifest_path}")
    if require_objects:
        _validate_object_presence(sync_root, manifest)
    return ResolvedSnapshot(
        manifest=manifest,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        head_path=None,
    )


def _observed_stamps(
    sync_root: Path,
    dataset: str,
    *,
    now_ns: int,
    max_clock_skew_seconds: int,
    require_objects: bool = True,
) -> list[HLC]:
    candidates, diagnostics, incomplete = _head_candidates(
        sync_root,
        dataset,
        now_ns=now_ns,
        max_clock_skew_seconds=max_clock_skew_seconds,
        require_objects=require_objects,
    )
    blocking = [message for _, message in incomplete]
    blocking.extend(item for item in diagnostics if "in the future" in item)
    if blocking:
        raise SnapshotError("; ".join(dict.fromkeys(blocking)))
    return [HLC.from_mapping(item.manifest["hlc"]) for item in candidates]


def _archive_d_primary_head(sync_root: Path, head_path: Path) -> str | None:
    """Preserve the replaced head bytes when D is the only cold history."""

    if not (sync_root / ".stockagent-d-primary").exists() or not head_path.exists():
        return None
    if head_path.is_symlink():
        raise SnapshotError("D primary head is redirected")
    from stockagent.data_sync.cold_primary import _check_d_primary_mount

    _check_d_primary_mount(sync_root)
    previous = head_path.read_bytes()
    digest = hashlib.sha256(previous).hexdigest()
    relative = Path("head-history") / head_path.relative_to(sync_root).with_suffix("") / f"{digest}.json"
    archived = sync_root / relative
    _ensure_shared_packed_directory(sync_root, archived.parent)
    if archived.is_symlink():
        raise SnapshotError("D primary head history is redirected")
    if archived.exists():
        if archived.read_bytes() != previous:
            raise SnapshotError("D primary head history checksum collision")
    else:
        atomic_write_bytes(archived, previous)
    return relative.as_posix()


def _store_chunked_source(
    sync_root: Path,
    source: Path,
    entry: _SourceEntry,
    *,
    chunk_bytes: int,
    created_objects: dict[str, dict[str, Any]],
    newly_installed_hashes: set[str],
    install_options: Mapping[str, Any],
    chunking: str = "fixed-v1",
) -> None:
    """Store exact bounded extents in the existing blob CAS, not another pool."""
    path = _entry_read_path(source, entry)
    _ensure_source_stat(path, entry)
    staging = sync_root / ".local-state" / "staging"
    chunks = []
    complete_hash = hashlib.sha256()
    total = 0
    with path.open("rb") as stream:
        for block in _source_blocks(stream, chunk_bytes, chunking):
            total += len(block)
            if total > entry.size:
                raise SnapshotError(f"source file grew while chunking: {path}")
            complete_hash.update(block)
            digest = hashlib.sha256(block).hexdigest()
            chunks.append({"object_sha256": digest, "bytes": len(block)})
            if digest in created_objects:
                continue
            relpath = _object_relpath("blobs", digest, ".blob")
            destination = sync_root.joinpath(*relpath.parts)
            _ensure_shared_packed_directory(sync_root, destination.parent)
            if destination.is_symlink():
                raise SnapshotError(f"content-addressed object is redirected: {destination}")
            if destination.exists():
                before = destination.lstat()
                if not stat.S_ISREG(before.st_mode) or before.st_size != len(block):
                    raise SnapshotError(f"content-addressed chunk is corrupt: {destination}")
                if (sha256_file(destination) != digest
                        or _source_stat(destination.lstat()) != _source_stat(before)):
                    raise SnapshotError(f"content-addressed chunk is corrupt: {destination}")
            else:
                temporary = staging / f"chunk-{uuid.uuid4().hex}.partial"
                with temporary.open("xb") as output:
                    output.write(block)
                    output.flush()
                    os.fsync(output.fileno())
                present = _install_immutable_object(
                    sync_root, temporary, destination, expected_sha256=digest, **install_options
                )
                if not present:
                    newly_installed_hashes.add(digest)
            created_objects[digest] = {
                "bytes": len(block), "file_count": 1, "kind": "blob",
                "logical_bytes": len(block), "relpath": relpath.as_posix(), "sha256": digest,
            }
    _ensure_source_stat(path, entry)
    actual_hash = complete_hash.hexdigest()
    if total != entry.size or (entry.sha256 is not None and actual_hash != entry.sha256):
        raise SnapshotError(f"source file changed while chunking: {path}")
    entry.sha256 = actual_hash
    entry.storage = {"kind": "chunks", "chunks": chunks}


def _source_blocks(stream, maximum: int, chunking: str):
    if chunking == "fixed-v1":
        while block := stream.read(maximum):
            yield block
        return
    if chunking != "fastcdc-v1" or maximum < 1024:
        raise SnapshotError("invalid bounded content-defined chunking policy")
    try:
        from fastcdc import fastcdc
        from importlib.metadata import version
    except ImportError as error:
        raise SnapshotError("fastcdc-v1 writer requires the declared native fastcdc dependency") from error
    if version("fastcdc") != "1.7.0" or fastcdc.__module__ != "fastcdc.fastcdc_cy":
        raise SnapshotError("fastcdc-v1 requires the pinned native implementation, not a Python fallback")
    # Do not mmap a mutable/huge source. Keep at most two maximum chunks plus
    # the yielded block; retaining the final cut until EOF avoids artificial
    # stream-buffer boundaries and preserves CDC's insertion resilience.
    pending = b""
    while True:
        incoming = stream.read(2 * maximum - len(pending))
        pending += incoming
        if not pending:
            return
        cuts = list(fastcdc(pending, min_size=maximum // 16,
                            avg_size=maximum // 4, max_size=maximum, fat=False))
        emitted = cuts if not incoming else cuts[:-1]
        consumed = 0
        for chunk in emitted:
            if chunk.offset != consumed or not 0 < chunk.length <= maximum:
                raise SnapshotError("native CDC emitted a discontinuous or unbounded extent")
            consumed += chunk.length
            yield pending[chunk.offset:consumed]
        pending = pending[consumed:]
        if not incoming:
            if pending:
                raise SnapshotError("native CDC did not consume the final source bytes")
            return


def publish_packed_snapshot(
    sync_root: Path,
    dataset: str,
    source: Path,
    *,
    node_id: str | None = None,
    loose_file_threshold_bytes: int = DEFAULT_LOOSE_FILE_THRESHOLD_BYTES,
    pack_buckets: int = DEFAULT_PACK_BUCKETS,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    metadata: Mapping[str, str] | None = None,
    excluded_subtrees: Iterable[str] = (),
    maximum_file_bytes: int | None = None,
    included_root_files: Iterable[str] | None = None,
    repo_root: Path | None = None,
    recover_missing_base_objects: bool = False,
    source_guard: Callable[[], None] | None = None,
    defer_scan: bool = False,
    batch_directory_fsync: bool = False,
    d_primary_native_blob_reads: bool = False,
    d_primary_native_blob_writes: bool = False,
    file_chunk_bytes: int | None = None,
    publication_lock_nonblocking: bool = False,
    file_chunking: str | None = None,
    bootstrap_file_storage: Mapping[str, Mapping[str, Any]] | MetadataRows[dict] | None = None,
    bootstrap_objects: Mapping[str, Mapping[str, Any]] | None = None,
    private_observation_cohort: bool = False,
    private_atomic_json_controls: bool = False,
    file_overrides: Mapping[str, Path] | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
    private_staging_file_rules: Iterable[str] = (),
    sealed_pack_hints: Any | None = None,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    source = source.resolve()
    dataset = validate_slug(dataset, "dataset")
    if type(batch_directory_fsync) is not bool:
        raise SnapshotError("batch_directory_fsync must be an explicit boolean")
    if type(publication_lock_nonblocking) is not bool:
        raise SnapshotError("publication_lock_nonblocking must be an explicit boolean")
    if type(private_atomic_json_controls) is not bool or (private_atomic_json_controls and not private_observation_cohort):
        raise SnapshotError('atomic JSON observations require an explicit private cohort')
    if type(d_primary_native_blob_reads) is not bool:
        raise SnapshotError("d_primary_native_blob_reads must be an explicit boolean")
    if d_primary_native_blob_reads and sync_root != Path("/srv/stockagent-packed"):
        raise SnapshotError("native D blob reads require the canonical enrolled D store")
    if type(d_primary_native_blob_writes) is not bool:
        raise SnapshotError("d_primary_native_blob_writes must be an explicit boolean")
    if d_primary_native_blob_writes:
        if sync_root != Path("/srv/stockagent-packed"):
            raise SnapshotError("native D blob writes require the canonical enrolled D store")
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        _check_d_primary_mount(sync_root)
    if _paths_overlap(sync_root, source):
        raise SnapshotError("snapshot source and packed sync root must not overlap")
    if loose_file_threshold_bytes < 1:
        raise SnapshotError("loose_file_threshold_bytes must be positive")
    if not 1 <= pack_buckets <= 4096:
        raise SnapshotError("pack_buckets must be between 1 and 4096")
    if not 0 <= compression_level <= 9:
        raise SnapshotError("compression_level must be between 0 and 9")
    if maximum_file_bytes is not None and maximum_file_bytes < 0:
        raise SnapshotError("maximum_file_bytes must be non-negative")
    if file_chunk_bytes is not None and (
        type(file_chunk_bytes) is not int or not 1 <= file_chunk_bytes <= MAX_FILE_CHUNK_BYTES
    ):
        raise SnapshotError("file_chunk_bytes must be an integer between 1 and 64 MiB")
    if file_chunking not in {None, "fixed-v1", "fastcdc-v1"}:
        raise SnapshotError("unknown file chunking policy")
    if file_chunking == "fastcdc-v1" and (file_chunk_bytes is None or file_chunk_bytes < 1024):
        raise SnapshotError("fastcdc-v1 requires a bounded maximum of at least 1024 bytes")
    if (bootstrap_file_storage is None) != (bootstrap_objects is None):
        raise SnapshotError("initial storage reuse requires both file and object bindings")
    if type(private_observation_cohort) is not bool:
        raise SnapshotError("private_observation_cohort must be an explicit boolean")
    staging_rules = _normalize_private_staging_rules(private_staging_file_rules)
    if staging_rules and not private_observation_cohort:
        raise SnapshotError("writer staging rules require a private observation cohort")
    if file_overrides and not private_observation_cohort:
        raise SnapshotError("file overrides require a private observation cohort")
    if sealed_pack_hints is not None:
        from stockagent.data_sync.packing_checkpoint import SealedPackHints, CONTRACT as HINT_CONTRACT
        if (not private_observation_cohort or not isinstance(sealed_pack_hints, SealedPackHints)
                or sealed_pack_hints.binding != {'contract': HINT_CONTRACT, 'cold': str(sync_root),
                                               'source': str(source), 'dataset': dataset}):
            raise SnapshotError("sealed-pack hints require their original private source/authority")
    if private_observation_cohort:
        marker = sync_root / "private-history.json"
        if (sync_root.name != "private-history" or sync_root.parent.name != ".local-state"
                or marker.is_symlink() or not marker.is_file()):
            raise SnapshotError("observation cohorts are restricted to the ignored private history")
        private_policy = _load_json(marker)
        if any(private_policy.get(name) is not False for name in (
                "shared_export", "nas_export", "source_retirement")):
            raise SnapshotError("observation cohort has no local-only privacy boundary")
        for name, path in (file_overrides or {}).items():
            _safe_relative_path(name, "frozen control name")
            if (not isinstance(path, Path) or path.absolute() != path or path.resolve() != path
                    or not path.is_file() or path.is_relative_to(sync_root)
                    or path.is_relative_to(source)):
                raise SnapshotError("frozen controls must be real private staging files outside source/cold")
    excluded = _normalize_excluded_subtrees(excluded_subtrees)
    included = _normalize_included_root_files(included_root_files)
    if included is not None and (excluded or maximum_file_bytes is not None):
        raise SnapshotError("explicit root files cannot be combined with omission filters")
    selected_options = {"included_root_files": included} if included is not None else {}
    node_identity = sync_root / ".local-state" / "node-id"
    initialize_packed_layout(
        sync_root,
        node_id=node_id if not node_identity.exists() else None,
    )
    publisher_node = _resolve_node_id(sync_root, node_id)
    lock_path = sync_root / ".local-state" / "locks" / f"publish-{dataset}.lock"
    retention_lock = (
        sync_root / ".local-state" / "locks" / "publish-retention-global.lock"
    )

    # Retention is exclusive; independent publishers share its existing inode.
    # Same-dataset heads remain exclusive. Hours of packing one source must not
    # block every other dataset. CAS promotion below never overwrites a winner.
    with contextlib.ExitStack() as publication_locks:
        publication_locks.enter_context(_try_publication_lock(retention_lock,
            shared=True, blocking=not publication_lock_nonblocking))
        if publication_lock_nonblocking:
            publication_locks.enter_context(_try_publication_lock(lock_path))
        else:
            publication_locks.enter_context(_exclusive_lock(lock_path))
        pending_directories: set[Path] | None = set() if batch_directory_fsync else None
        install_options = {"pending_directories": pending_directories} if batch_directory_fsync else {}
        selected_root = _source_stat(source.lstat())[:2] if included is not None or private_observation_cohort else None
        entries = publication_locks.enter_context(MetadataRows(
            _entry_record, _entry_from_record, memory_rows=METADATA_MEMORY_ROWS))
        entries, before = _collect_entries(
            source,
            excluded_subtrees=excluded,
            maximum_file_bytes=maximum_file_bytes,
            **({"file_overrides": file_overrides} if file_overrides else {}),
            **({'atomic_json_controls': True} if private_atomic_json_controls else {}),
            **({"progress": progress} if progress is not None else {}),
            **({"private_staging_file_rules": staging_rules} if staging_rules else {}),
            entry_sink=entries,
            **selected_options,
        )
        # Unrelated key creation must not invalidate this deliberately narrow
        # release, but replacing the actual source directory must never escape
        # the selected-file stability check.
        if selected_root is not None and _source_stat(source.lstat())[:2] != selected_root:
            raise SnapshotError("selected snapshot source directory was replaced")
        if source_guard is not None:
            source_guard()
        staging_root = sync_root / ".local-state" / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        previous: ResolvedSnapshot | None = None
        previous_files = publication_locks.enter_context(MetadataRows(
            lambda row: row, lambda row: row, memory_rows=METADATA_MEMORY_ROWS))
        previous_objects: dict[str, dict[str, Any]] = {}
        missing_base_objects: list[dict[str, Any]] = []
        if any((sync_root / "heads" / dataset).glob("*.json")):
            previous = resolve_latest_packed(
                sync_root,
                dataset,
                max_clock_skew_seconds=max_clock_skew_seconds,
                require_objects=not recover_missing_base_objects,
            )
            previous_files.extend(_iter_inventory(sync_root, previous.manifest))
            _validate_inventory(previous.manifest, previous_files, retain_object_members=False)
            previous_objects = {
                str(item["sha256"]): dict(item)
                for item in previous.manifest["archive"]["objects"]
            }
            # Once this dataset has entered v2, ordinary scheduled publications
            # must keep using its extent policy. Omitting an opt-in flag on the
            # next run is not a request to resume whole-file history copies.
            if (file_chunk_bytes is None
                    and previous.manifest["schema_version"] == CHUNKED_SNAPSHOT_SCHEMA_VERSION):
                file_chunk_bytes = int(previous.manifest["archive"].get("file_chunk_bytes")
                                       or DEFAULT_FILE_CHUNK_BYTES)
            if file_chunking is None:
                file_chunking = previous.manifest["archive"].get("file_chunking", "fixed-v1")
            if recover_missing_base_objects:
                # Reuse only objects which still exist. Missing base objects
                # make ALL their current source members changed files. The
                # regular content-addressed writer creates fresh objects; it
                # never puts new bytes under a missing old digest. Old
                # manifests/inventories are retained as recovery evidence.
                for digest, item in list(previous_objects.items()):
                    object_path = sync_root.joinpath(*PurePosixPath(item["relpath"]).parts)
                    if not object_path.is_file():
                        missing_base_objects.append(dict(item))
                        del previous_objects[digest]

        if previous is None and bootstrap_file_storage is not None:
            # Hints are not integrity evidence. Every current source is still
            # fully hashed below and every reused extent must be present in
            # this same authority; independent reconstruction follows commit.
            if isinstance(bootstrap_file_storage, MetadataRows):
                # Transfer this disposable index, not N decoded rows into a
                # second index. Its caller must not reuse it after publishing;
                # ExitStack closes it on success AND every failure path.
                previous_files.close()
                previous_files = publication_locks.enter_context(bootstrap_file_storage)
                hints = ((row.get("path"), row) for row in previous_files)
            else:
                hints = bootstrap_file_storage.items()
            for name, row in hints:
                if (_safe_relative_path(name, "bootstrap path").as_posix() != name
                        or row.get("kind") != "file" or row.get("path") != name):
                    raise SnapshotError("invalid initial file storage hint")
                if not isinstance(bootstrap_file_storage, MetadataRows):
                    previous_files.append(dict(row))
            previous_objects = {str(key): dict(value) for key, value in bootstrap_objects.items()}
            for digest, item in previous_objects.items():
                if (digest != _validate_hash(item.get("sha256"), "bootstrap object")
                        or not _path_under(sync_root, item["relpath"], "bootstrap object").is_file()):
                    raise SnapshotError("initial storage object is missing or mismatched")
        file_chunking = file_chunking or "fixed-v1"
        if sealed_pack_hints is not None:
            # Only incomplete first-baseline work needs uncommitted hints.
            # Never replace an existing committed inventory's preferred rows.
            sealed_pack_hints.load_into(previous_files if previous is None else None, previous_objects)

        reused_files = 0
        changed_file_count = 0
        # Previously referenced objects were checked for presence/length by
        # resolution. Reusing one after independently hashing the corresponding
        # source extent is the same contract as whole-file no-op reuse. Full
        # cold-byte recovery verification remains a separate required gate.
        created_objects: dict[str, dict[str, Any]] = dict(previous_objects)
        newly_installed_hashes: set[str] = set()
        completed_files = completed_bytes = routed_files = 0
        last_progress = 0.0
        def report(stage, *, force=False, phase_completed=None, phase_total=None):
            nonlocal last_progress
            if progress is not None and (force or time.monotonic() - last_progress >= 5):
                last_progress = time.monotonic()
                progress({"stage": stage, "files_completed": completed_files,
                          "files_total": before["files"], "logical_bytes_processed": completed_bytes,
                          "logical_bytes_total": before["logical_bytes"],
                          "files_routed": routed_files, "files_hashed": completed_files,
                          "phase_completed_items": phase_completed, "phase_total_items": phase_total,
                          **({"sealed_pack_hints": dict(sealed_pack_hints.statistics)}
                             if sealed_pack_hints is not None else {}),
                          "new_objects": len(newly_installed_hashes), "acceptance": False})
        report("routing_and_hashing", force=True)
        for entry in entries:
            if entry.kind != "file":
                continue
            censused_size = entry.size
            # Paths are unique and each hint is consumed once. Release its old
            # row as we go instead of retaining two full file inventories until
            # the final source has been hashed (which can force HDD swap).
            prior = previous_files.take_file_hint(entry.path)
            prior_storage = prior.get("storage") if prior else None
            # A first baseline (or a size-changing file / already-chunked
            # version) need not read its entire source twice. Hash the complete
            # original AND every extent while installing missing bytes in one
            # bounded pass. Keep unchanged legacy blobs/packs unchanged.
            one_pass = (not entry.atomic_json_control and file_chunk_bytes is not None and entry.size > file_chunk_bytes
                and entry.size >= loose_file_threshold_bytes
                and (prior is None or int(prior.get("size", -1)) != entry.size
                     or (isinstance(prior_storage, Mapping) and prior_storage.get("kind") == "chunks"
                         and previous is not None
                         and previous.manifest["archive"].get("file_chunking", "fixed-v1") == file_chunking
                         and previous.manifest["archive"].get("file_chunk_bytes") == file_chunk_bytes)))
            new_small_file = prior is None and (entry.atomic_json_control or entry.size < loose_file_threshold_bytes)
            if new_small_file:
                # No donor exists, so reading this file now cannot discover
                # reuse. Its ONE complete SHA/read happens while writing ZIP,
                # under the same before/after source-stat fences below.
                pass
            elif one_pass:
                _store_chunked_source(sync_root, source, entry, chunk_bytes=file_chunk_bytes,
                    created_objects=created_objects, newly_installed_hashes=newly_installed_hashes,
                    install_options=install_options, chunking=file_chunking)
            else:
                entry.sha256 = _hash_source_entry(source, entry, native_d_io=d_primary_native_blob_reads)
            if entry.atomic_json_control:
                before["logical_bytes"] += entry.size - censused_size
            reused = (
                prior is not None
                and int(prior.get("size", -1)) == entry.size
                and prior.get("sha256") == entry.sha256
                and isinstance(prior_storage, Mapping)
                and all(digest in previous_objects
                        for digest, _ in _storage_extents(prior_storage, entry.size))
            )
            if reused:
                entry.storage = dict(prior_storage)
                reused_files += 1
            else:
                changed_file_count += 1
            entries.update(entry, changed_bucket=(
                (-1 if entry.size >= loose_file_threshold_bytes and not entry.atomic_json_control
                 else _bucket_for_path(entry.path, pack_buckets))
                if not reused else None))
            routed_files += 1
            if not new_small_file:
                completed_files += 1
                completed_bytes += entry.size
            report("routing_and_hashing")
        report("routing_and_hashing", force=True)

        # Every current entry owns its selected storage mapping. Old per-file
        # hints are no longer needed while packing and validating the new one.
        previous_files.close()

        for entry in entries.changed(-1):
            if entry.storage is not None:
                continue
            if file_chunk_bytes is not None and entry.size > file_chunk_bytes:
                _store_chunked_source(sync_root, source, entry, chunk_bytes=file_chunk_bytes,
                    created_objects=created_objects, newly_installed_hashes=newly_installed_hashes,
                    install_options=install_options, chunking=file_chunking)
                entries.update(entry)
                continue
            source_path = _entry_read_path(source, entry)
            _ensure_source_stat(source_path, entry)
            digest = str(entry.sha256)
            relpath = _object_relpath("blobs", digest, ".blob")
            destination = sync_root.joinpath(*relpath.parts)
            _ensure_shared_packed_directory(sync_root, destination.parent)
            if destination.is_symlink():
                raise SnapshotError(f"content-addressed object is redirected: {destination}")
            if destination.exists():
                # The source has already been fully hashed under its stability
                # signature. Verify the existing immutable bytes BEFORE reuse;
                # copying an identical blob to D scratch only to unlink it
                # afterwards adds no recovery proof and wastes a full write.
                observed = destination.lstat()
                read_path = destination
                if d_primary_native_blob_reads:
                    from stockagent.data_sync.cold_primary import d_primary_read_alias
                    from stockagent.data_sync.windows_cold_io import hash_file
                    read_path = d_primary_read_alias(destination)
                # A one-shot, closed D preservation payload may already be the
                # very same NTFS inode as its installed object. _hash_source_entry
                # has just hashed every byte under the source signature; the
                # canonical D alias/volume/file-ID check and final signature
                # rechecks prove that hashing this inode again adds no evidence.
                same_verified_inode = d_primary_native_blob_reads and os.path.samestat(
                    source_path.lstat(), read_path.lstat())
                if (
                    not stat.S_ISREG(observed.st_mode)
                    or observed.st_size != entry.size
                    or (not same_verified_inode and (
                        hash_file(read_path) if d_primary_native_blob_reads else sha256_file(read_path)
                    ) != digest)
                ):
                    raise SnapshotError(f"content-addressed object is corrupt: {destination}")
                if _source_stat(destination.lstat()) != _source_stat(observed):
                    raise SnapshotError(f"content-addressed object changed during reuse: {destination}")
                _ensure_source_stat(source_path, entry)
                already_present = True
            else:
                temporary = staging_root / f"blob-{uuid.uuid4().hex}.partial"
                # Interleaved real C-to-D write/full-readback measurements on
                # penguin favor DrvFs at 64 MiB and native FileStream at 512 MiB.
                # Keep process startup off smaller copies; other callers keep
                # the established default unless they explicitly enable it.
                copied_digest = (_native_copy_and_hash(source_path, temporary)
                                 if d_primary_native_blob_writes and entry.size >= 512 * 1024**2
                                 else _copy_and_hash(source_path, temporary))
                _ensure_source_stat(source_path, entry)
                if copied_digest != digest:
                    raise SnapshotError(f"source file changed while packing: {source_path}")
                already_present = _install_immutable_object(
                    sync_root, temporary, destination, expected_sha256=digest, **install_options
                )
            if not already_present:
                newly_installed_hashes.add(digest)
            entry.sha256 = digest
            entry.storage = {
                "kind": "blob",
                "object_sha256": digest,
            }
            created_objects[digest] = {
                    "bytes": entry.size,
                    "file_count": 1,
                    "kind": "blob",
                    "logical_bytes": entry.size,
                    "relpath": relpath.as_posix(),
                    "sha256": digest,
                }
            entries.update(entry)

        buckets = [(bucket, count, size) for bucket, count, size in entries.changed_groups() if bucket >= 0]
        report("packing_changed_small_files", force=True, phase_completed=0, phase_total=len(buckets))
        for bucket_index, (bucket, _, _) in enumerate(buckets, start=1):
            bucket_rows = entries.changed(bucket)
            while bucket_entries := list(itertools.islice(bucket_rows, MAX_PACK_MEMBERS)):
                unhashed = {entry.path for entry in bucket_entries if entry.sha256 is None}
                observed_sizes = {entry.path: entry.size for entry in bucket_entries
                                  if entry.atomic_json_control}
                def packed_entry(entry):
                    nonlocal completed_files, completed_bytes
                    size_delta = entry.size - observed_sizes.get(entry.path, entry.size)
                    before["logical_bytes"] += size_delta
                    if entry.path in unhashed:
                        completed_files += 1
                        completed_bytes += entry.size
                    else:
                        completed_bytes += size_delta
                    report("packing_changed_small_files", phase_completed=bucket_index - 1,
                           phase_total=len(buckets))
                temporary = staging_root / f"pack-{bucket:04d}-{uuid.uuid4().hex}.partial"
                _write_pack(source, bucket_entries, temporary, compression_level=compression_level,
                            on_entry=packed_entry)
                digest = sha256_file(temporary)
                relpath = _object_relpath("packs", digest, ".zip")
                destination = sync_root.joinpath(*relpath.parts)
                size = temporary.stat().st_size
                already_present = _install_immutable_object(
                    sync_root, temporary, destination, expected_sha256=digest, **install_options)
                if not already_present:
                    newly_installed_hashes.add(digest)
                logical_bytes = sum(entry.size for entry in bucket_entries)
                for entry in bucket_entries:
                    entry.storage = {"kind": "pack", "member": entry.path, "object_sha256": digest}
                    entries.update(entry)
                created_objects[digest] = {
                    "bucket": bucket,
                    "bytes": size,
                    "file_count": len(bucket_entries),
                    "kind": "pack",
                    "logical_bytes": logical_bytes,
                    "relpath": relpath.as_posix(),
                    "sha256": digest,
                }
                if sealed_pack_hints is not None and previous is None:
                    # File bytes were fsynced on CAS install. Persist names as
                    # well BEFORE advertising a sealed pack in the journal.
                    if pending_directories:
                        _flush_object_directories(pending_directories)
                        pending_directories.clear()
                    sealed_pack_hints.record(created_objects[digest], bucket_entries)
                report("packing_changed_small_files", phase_completed=bucket_index, phase_total=len(buckets))

        report("verifying_complete_file_storage", force=True,
               phase_completed=0, phase_total=len(entries))
        observed_controls = 0
        observed_digest = hashlib.sha256()
        observed_logical_bytes = 0
        for entry_index, entry in enumerate(entries, 1):
            if entry.kind == "file" and (not entry.sha256 or not entry.storage):
                raise SnapshotError(
                    f"file was not assigned to a packed object: {entry.path}"
                )
            if entry.kind == 'file':
                observed_logical_bytes += entry.size
            if entry.atomic_json_control:
                observed_controls += 1
                observed_digest.update(json.dumps({'path': entry.path, 'mode': entry.mode,
                    'size': entry.size, 'sha256': entry.sha256}, sort_keys=True,
                    separators=(',', ':')).encode() + b'\n')
            if entry_index % 1024 == 0:
                report("verifying_complete_file_storage", phase_completed=entry_index,
                       phase_total=len(entries))
        if private_atomic_json_controls:
            # Retain the ORIGINAL portable scan_tree ABI, including its walk
            # order. A metadata-only ordered pass uses sealed sizes/targets,
            # never a second current-source observation or a weaker fingerprint.
            before['logical_bytes'] = observed_logical_bytes
            portable = hashlib.sha256()
            for ordinal, entry in enumerate(entries.in_census_order()):
                if type(entry.census_order) is not int or entry.census_order != ordinal:
                    raise SnapshotError('observed cohort lost its complete census order')
                kind = {'file': 'F', 'directory': 'D', 'symlink': 'L'}[entry.kind]
                extra = entry.target or ''
                portable.update(kind.encode('ascii') + b'\0' + entry.path.encode('utf-8', errors='surrogateescape') + b'\0')
                portable.update(str(entry.size if entry.kind == 'file' else 0).encode('ascii')
                                + b'\0' + extra.encode('utf-8', errors='surrogateescape') + b'\n')
            before['portable_fingerprint_sha256'] = portable.hexdigest()

        report("committing_complete_inventory", force=True)
        inventory_temp = staging_root / f"inventory-{uuid.uuid4().hex}.partial"
        inventory_sha = _write_inventory(inventory_temp, entries)
        inventory_relpath = _object_relpath("inventories", inventory_sha, ".jsonl.gz")
        inventory_path = sync_root.joinpath(*inventory_relpath.parts)
        inventory_bytes = inventory_temp.stat().st_size
        inventory_already_present = _install_immutable_object(
            sync_root, inventory_temp, inventory_path, expected_sha256=inventory_sha, **install_options
        )

        if private_observation_cohort:
            _guard_observation_cohort(source, entries, selected_root)
            after = before
        else:
            report("verifying_source_stability", force=True,
                   phase_completed=0, phase_total=before["files"])
            def stability_progress(event):
                report("verifying_source_stability", phase_completed=event["files_completed"],
                       phase_total=before["files"])
            _, after = _collect_entries(
                source,
                excluded_subtrees=excluded,
                maximum_file_bytes=maximum_file_bytes,
                retain_entries=False,
                **({"progress": stability_progress} if progress is not None else {}),
                **selected_options,
            )
        if selected_root is not None and _source_stat(source.lstat())[:2] != selected_root:
            raise SnapshotError("selected snapshot source directory was replaced")
        if (
            before["stability_fingerprint_sha256"]
            != after["stability_fingerprint_sha256"]
        ):
            raise SnapshotError(
                "source tree changed while it was being packed; publish from a frozen "
                "snapshot or under the downloader's dataset lock"
            )
        # An optional producer proof binds these fully hashed source bytes to
        # the inputs used to build their derived tables. It supplements, never
        # replaces, the complete inventory and source-stability checks above.
        if source_guard is not None:
            source_guard()
        report("source_fences_verified", force=True)

        if pending_directories:
            _flush_object_directories(pending_directories)

        # The immutable inventory is the semantic dataset identity.  Reusing
        # the previous release when it is identical prevents a no-op publish
        # from advancing the head and forcing every consumer to materialize the
        # same 100k+ files under a fresh timestamp-only snapshot ID.
        if (
            previous is not None
            # Explicit reader enrollment is a real format transition, even
            # when every payload is reused. Commit only the new manifest once
            # so ordinary scheduled runs inherit v2 thereafter.
            and previous.manifest["schema_version"] == (
                CHUNKED_SNAPSHOT_SCHEMA_VERSION if file_chunk_bytes is not None
                else PACKED_SNAPSHOT_SCHEMA_VERSION)
            and previous.manifest["source"].get("selection", {}).get("included_root_files")
            == (list(included) if included is not None else None)
            and previous.manifest["source"].get("selection", {}).get("private_observation_cohort", False)
            == private_observation_cohort
            and previous.manifest['source'].get('selection', {}).get('atomic_json_observation_contract')
            == ('named_stable_fd_json_v2' if private_atomic_json_controls else None)
            and previous.manifest["source"].get("selection", {}).get("private_staging_file_rules", [])
            == list(staging_rules)
            and (included is None or previous.manifest.get("metadata", {}) == dict(metadata or {}))
            and previous.manifest.get("metadata", {}).get("version_dependencies_sha256")
            == (metadata or {}).get("version_dependencies_sha256")
            and previous.manifest["archive"].get("file_chunking", "fixed-v1") == file_chunking
            and str(previous.manifest["archive"]["inventory"]["sha256"])
            == inventory_sha
        ):
            if recover_missing_base_objects:
                verify_packed_snapshot(sync_root, previous)
            # Transport does not read mutable source bytes. A no-op must not
            # retain global publication locks through external scan requests.
            publication_locks.close()
            _notify_packed_publication(sync_root, dataset, defer_scan=defer_scan, retry_full=True)
            return previous

        wall_time_ns = time.time_ns()
        stamp = next_hlc(
            _observed_stamps(
                sync_root,
                dataset,
                now_ns=wall_time_ns,
                max_clock_skew_seconds=max_clock_skew_seconds,
                require_objects=not recover_missing_base_objects,
            ),
            node_id=publisher_node,
            now_ns=wall_time_ns,
        )
        seconds, nanos = divmod(stamp.physical_ns, 1_000_000_000)
        timestamp_slug = (
            time.strftime("%Y%m%dT%H%M%S", time.gmtime(seconds)) + f"{nanos:09d}Z"
        )
        snapshot_id = validate_slug(
            f"{dataset[:40]}-{timestamp_slug}-l{stamp.logical}-"
            f"{publisher_node[:40]}-{inventory_sha[:16]}",
            "snapshot_id",
        )
        object_usage: dict[str, dict[str, int]] = {}
        for entry in entries:
            if entry.kind != "file" or not entry.storage:
                continue
            per_file: dict[str, int] = {}
            for digest, count in _storage_extents(entry.storage, entry.size):
                per_file[digest] = per_file.get(digest, 0) + count
            for digest, count in per_file.items():
                usage = object_usage.setdefault(digest, {"file_count": 0, "logical_bytes": 0})
                usage["file_count"] += 1
                usage["logical_bytes"] += count
        unique_objects: dict[str, dict[str, Any]] = {}
        for digest, usage in object_usage.items():
            original = created_objects.get(digest) or previous_objects.get(digest)
            if original is None:
                raise SnapshotError(
                    f"inventory references an unavailable packed object: {digest}"
                )
            item = dict(original)
            stored_file_count = int(
                item.get("stored_file_count", item.get("file_count", -1))
            )
            stored_logical_bytes = int(
                item.get("stored_logical_bytes", item.get("logical_bytes", -1))
            )
            item["file_count"] = usage["file_count"]
            item["logical_bytes"] = usage["logical_bytes"]
            if item["kind"] == "pack" and (
                item.get("member_selection") == "subset"
                or usage["file_count"] != stored_file_count
            ):
                item["member_selection"] = "subset"
                item["stored_file_count"] = stored_file_count
                item["stored_logical_bytes"] = stored_logical_bytes
            unique_objects[digest] = item
        objects = sorted(
            unique_objects.values(),
            key=lambda item: (str(item["kind"]), str(item["sha256"])),
        )
        # Keep the declared policy even when the current release has only small
        # files (or is empty). Otherwise deleting the last large file would
        # silently reset the next scheduled batch to whole-file v1 storage.
        chunked = file_chunk_bytes is not None
        manifest: dict[str, Any] = {
            "schema_version": (CHUNKED_SNAPSHOT_SCHEMA_VERSION if chunked else PACKED_SNAPSHOT_SCHEMA_VERSION),
            "dataset": dataset,
            "snapshot_id": snapshot_id,
            "published_at": _utc_iso_from_ns(wall_time_ns),
            "hlc": stamp.to_dict(),
            "publisher": {"node_id": publisher_node},
            "source": {
                "snapshot_root_name": source.name,
                "excluded_subtrees": list(excluded),
                "selection": {
                    "maximum_file_bytes": maximum_file_bytes,
                    "symlinks": "included" if included is None else "excluded",
                    **({"included_root_files": list(included)} if included is not None else {}),
                    **({"private_observation_cohort": True,
                        "consistency": "fixed_observed_files_not_atomic_workspace",
                        "frozen_control_files": sorted(file_overrides or {})}
                       if private_observation_cohort else {}),
                    **({'atomic_json_observation_contract': 'named_stable_fd_json_v2',
                        'atomic_json_observed_files': observed_controls,
                        'atomic_json_observations_sha256': observed_digest.hexdigest()}
                       if private_atomic_json_controls else {}),
                    **({"private_staging_file_rules": list(staging_rules)} if staging_rules else {}),
                },
                **{
                    key: value
                    for key, value in before.items()
                    if key != "stability_fingerprint_sha256"
                },
            },
            "archive": {
                "format": CHUNKED_ARCHIVE_FORMAT if chunked else PACKED_ARCHIVE_FORMAT,
                "inventory": {
                    "bytes": inventory_bytes,
                    "format": INVENTORY_FORMAT,
                    "relpath": inventory_relpath.as_posix(),
                    "sha256": inventory_sha,
                },
                "loose_file_threshold_bytes": loose_file_threshold_bytes,
                **({"file_chunk_bytes": file_chunk_bytes} if chunked else {}),
                **({"file_chunking": file_chunking} if chunked else {}),
                "pack_buckets": pack_buckets,
                "compression": "deflate-or-store-by-extension",
                "compression_level": compression_level,
                "objects": objects,
                "object_count": len(objects),
                "stored_bytes": sum(int(item["bytes"]) for item in objects)
                + inventory_bytes,
                "base_snapshot_id": (
                    previous.manifest["snapshot_id"] if previous else None
                ),
                **({"base_missing_objects_repacked_from_current_source": missing_base_objects}
                   if missing_base_objects else {}),
                "reused_files": reused_files,
                "changed_files": changed_file_count,
                "new_object_count": len(newly_installed_hashes)
                + (0 if inventory_already_present else 1),
                "new_stored_bytes": sum(
                    int(created_objects[digest]["bytes"])
                    for digest in newly_installed_hashes
                )
                + (0 if inventory_already_present else inventory_bytes),
            },
            "metadata": dict(sorted((metadata or {}).items())),
        }
        if git_state := _git_state(repo_root):
            manifest["git"] = git_state
        manifest_relpath = PurePosixPath("manifests") / dataset / f"{snapshot_id}.json"
        manifest_path = sync_root.joinpath(*manifest_relpath.parts)
        _ensure_shared_packed_directory(sync_root, manifest_path.parent)
        manifest_sha = write_immutable_json(manifest_path, manifest)
        if recover_missing_base_objects:
            # Recovery must independently verify every selected object and
            # member BEFORE replacing this node's head, including intact
            # objects reused from the damaged release. Fail closed on any
            # checksum mismatch; do not overwrite corrupted existing bytes.
            verify_packed_snapshot(sync_root, ResolvedSnapshot(
                manifest=manifest, manifest_path=manifest_path,
                manifest_sha256=manifest_sha,
                head_path=sync_root / "heads" / dataset / f"{publisher_node}.json"))
        head = {
            "schema_version": PACKED_HEAD_SCHEMA_VERSION,
            "dataset": dataset,
            "node_id": publisher_node,
            "snapshot_id": snapshot_id,
            "hlc": stamp.to_dict(),
            "updated_at": _utc_iso_from_ns(wall_time_ns),
            "manifest_relpath": manifest_relpath.as_posix(),
            "manifest_sha256": manifest_sha,
        }
        head_path = sync_root / "heads" / dataset / f"{publisher_node}.json"
        _ensure_shared_packed_directory(sync_root, head_path.parent)
        archived_head = _archive_d_primary_head(sync_root, head_path)
        if selected_root is not None and _source_stat(source.lstat())[:2] != selected_root:
            raise SnapshotError("selected snapshot source directory was replaced before commit")
        if source_guard is not None:
            source_guard()
        atomic_write_json(head_path, head)
        published = ResolvedSnapshot(
            manifest=manifest,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha,
            head_path=head_path,
        )
        scan_paths = tuple(
            str(created_objects[digest]["relpath"])
            for digest in sorted(newly_installed_hashes)
        ) + (() if inventory_already_present else (inventory_relpath.as_posix(),))
        if archived_head is not None:
            scan_paths += (archived_head,)
    _notify_packed_publication(sync_root, dataset, new_object_paths=scan_paths, defer_scan=defer_scan)
    return published


def _notify_packed_publication(
    sync_root: Path, dataset: str, *, new_object_paths: tuple[str, ...] = (),
    defer_scan: bool = False, retry_full: bool = False,
) -> None:
    from stockagent.data_sync.syncthing_scan import queue_after_publish, scan_after_publish

    if defer_scan:
        # The caller can release its source lease only after this durable
        # notification and the unique commit receipt have both succeeded.
        if not queue_after_publish(sync_root, dataset, new_object_paths=new_object_paths):
            raise SnapshotError("publication committed but durable scan intent was not queued")
    else:
        acknowledged = scan_after_publish(
            sync_root, dataset, new_object_paths=new_object_paths, retry_full=retry_full,
        )
        if not acknowledged and (sync_root / ".local-state/scan-pending" / f"{dataset}.json").exists():
            raise SnapshotError("publication committed but a newer scan notification remains pending")


def _load_inventory(
    sync_root: Path, manifest: Mapping[str, Any]
) -> list[dict[str, Any]]:
    return list(_iter_inventory(sync_root, manifest))


def _iter_inventory(sync_root: Path, manifest: Mapping[str, Any]):
    descriptor = manifest["archive"]["inventory"]
    path = _path_under(sync_root, str(descriptor["relpath"]), "inventory relpath")
    expected_sha = _validate_hash(descriptor["sha256"], "inventory SHA-256")
    authority = Path('/srv/stockagent-packed')
    native = (sync_root in {authority, authority / '.local-state/private-history'}
              and (authority / '.stockagent-d-primary').is_file()
              and int(descriptor['bytes']) >= 8 * 1024**2)
    if not native and sha256_file(path) != expected_sha:
        raise SnapshotError(f"inventory checksum mismatch: {path}")
    # Reuse the exact, bounded native-byte adapter: complete compressed SHA,
    # stable inode/stat fence, then seekable SSD scratch for gzip/JSON decoding.
    # Avoid two slow 9p reads of a large inventory. Non-D/small inputs keep their
    # portable reader. Scratch pressure retains the adapter's verified fallback.
    source = (_native_pack_source(path, {'bytes': int(descriptor['bytes']), 'sha256': expected_sha},
                                  enabled=True, prefer_memory=False)
              if native else contextlib.nullcontext(path))
    previous_path = ""
    try:
        with source as compressed, gzip.open(compressed, mode="rt", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise SnapshotError(f"inventory row {line_number} is not an object")
                relative = _safe_relative_path(
                    str(row.get("path", "")), f"inventory row {line_number} path"
                ).as_posix()
                if relative <= previous_path:
                    raise SnapshotError("inventory paths are duplicated or not sorted")
                previous_path = relative
                row["path"] = relative
                yield row
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read packed inventory {path}: {exc}") from exc


@contextlib.contextmanager
def _open_inventory(sync_root: Path, manifest: Mapping[str, Any]):
    with MetadataRows(lambda row: row, lambda row: row, memory_rows=METADATA_MEMORY_ROWS) as rows:
        rows.extend(_iter_inventory(sync_root, manifest))
        yield rows


def _validate_inventory(
    manifest: Mapping[str, Any], entries: Iterable[dict[str, Any]], *,
    retain_object_members: bool = True,
) -> dict[str, Any]:
    object_by_hash = {
        str(item["sha256"]): item for item in manifest["archive"]["objects"]
    }
    counts = {"files": 0, "directories": 0, "symlinks": 0, "logical_bytes": 0}
    keep_members = retain_object_members and not isinstance(entries, MetadataRows)
    object_members: dict[str, Any] = {digest: [] for digest in object_by_hash} if keep_members else {}
    member_counts = dict.fromkeys(object_by_hash, 0)
    object_logical_bytes = dict.fromkeys(object_by_hash, 0)
    chunk_objects, direct_blob_objects = set(), set()
    chunked_files = chunked_logical_bytes = 0
    expected = manifest["source"]
    selection = expected.get("selection", {})
    if not isinstance(selection, Mapping):
        raise SnapshotError("invalid snapshot source selection")
    staging_rules = ()
    if "private_staging_file_rules" in selection:
        staging_rules = _normalize_private_staging_rules(selection["private_staging_file_rules"])
        if (selection.get("private_observation_cohort") is not True
                or list(staging_rules) != selection["private_staging_file_rules"]):
            raise SnapshotError("inventory differs from its private writer staging boundary")
    included = None
    included_paths = []
    if "included_root_files" in selection:
        if not isinstance(selection["included_root_files"], list):
            raise SnapshotError("included root file inventory requires a list")
        included = _normalize_included_root_files(selection["included_root_files"])
        if (list(included) != selection["included_root_files"]
                or selection.get("symlinks") != "excluded"
                or selection.get("maximum_file_bytes") is not None or expected.get("excluded_subtrees")):
            raise SnapshotError("inventory differs from its explicit root-file selection")
    for row in entries:
        kind = row.get("kind")
        if staging_rules and kind != "directory" and _is_private_writer_staging(row["path"], staging_rules):
            raise SnapshotError("inventory differs from its private writer staging boundary")
        if included is not None:
            if kind != "file" or len(included_paths) >= len(included):
                raise SnapshotError("inventory differs from its explicit root-file selection")
            included_paths.append(row["path"])
        mode = int(row.get("mode", -1))
        if not 0 <= mode <= 0o7777:
            raise SnapshotError(f"invalid mode for inventory path {row['path']}")
        if kind == "directory":
            counts["directories"] += 1
            continue
        if kind == "symlink":
            target = str(row.get("target", ""))
            _validate_symlink_target(str(row["path"]), target)
            counts["symlinks"] += 1
            continue
        if kind != "file":
            raise SnapshotError(f"invalid inventory kind for {row['path']}: {kind!r}")
        size = int(row.get("size", -1))
        digest = _validate_hash(row.get("sha256"), "file SHA-256")
        storage = row.get("storage")
        if size < 0 or not isinstance(storage, Mapping):
            raise SnapshotError(f"invalid file inventory row for {row['path']}")
        storage_kind = storage.get("kind")
        if storage_kind == "pack" and storage.get("member") != row["path"]:
            raise SnapshotError(f"pack member/path mismatch for {row['path']}")
        if storage_kind not in {"pack", "blob", "chunks"}:
            raise SnapshotError(f"invalid storage kind for {row['path']}")
        if storage_kind == "chunks" and manifest["schema_version"] != CHUNKED_SNAPSHOT_SCHEMA_VERSION:
            raise SnapshotError("chunked files require the v2 archive contract")
        if storage_kind == "chunks":
            chunked_files += 1
            chunked_logical_bytes += size
        seen = set()
        for object_sha, count in _storage_extents(storage, size):
            if object_sha not in object_by_hash:
                raise SnapshotError(f"inventory references an unknown object: {object_sha}")
            item = object_by_hash[object_sha]
            if item["kind"] != ("blob" if storage_kind == "chunks" else storage_kind):
                raise SnapshotError(f"inventory/object kind mismatch for {row['path']}")
            if storage_kind == "blob" and digest != object_sha:
                raise SnapshotError(f"blob/file checksum mismatch for {row['path']}")
            if storage_kind in {"blob", "chunks"} and count != item["bytes"]:
                raise SnapshotError(f"blob extent size mismatch for {row['path']}")
            if storage_kind == "chunks":
                chunk_objects.add(object_sha)
            elif storage_kind == "blob":
                direct_blob_objects.add(object_sha)
            if object_sha not in seen:
                member_counts[object_sha] += 1
                if keep_members:
                    object_members[object_sha].append(str(row["path"]))
                seen.add(object_sha)
            object_logical_bytes[object_sha] += count
        counts["files"] += 1
        counts["logical_bytes"] += size
        row["sha256"] = digest
    if included is not None and sorted(included_paths) != list(included):
        raise SnapshotError("inventory differs from its explicit root-file selection")
    for key, value in counts.items():
        if int(expected.get(key, -1)) != value:
            raise SnapshotError(
                f"inventory {key} mismatch: expected {expected.get(key)}, got {value}"
            )
    for digest, item in object_by_hash.items():
        members = member_counts[digest]
        if members != int(item["file_count"]):
            raise SnapshotError(f"object file count mismatch: {digest}")
        if object_logical_bytes[digest] != int(item["logical_bytes"]):
            raise SnapshotError(f"object logical byte count mismatch: {digest}")
        if item.get("member_selection") == "subset":
            stored_file_count = int(item.get("stored_file_count", -1))
            if item["kind"] != "pack" or not members <= stored_file_count:
                raise SnapshotError(f"invalid subset pack descriptor: {digest}")
        if item["kind"] == "blob" and not members:
            raise SnapshotError(f"blob object has no inventory references: {digest}")
    if retain_object_members and isinstance(entries, MetadataRows):
        def paths_for_object(digest):
            return (row["path"] for row in entries.object_rows(digest))
        object_members = {digest: paths_for_object(digest) for digest, item in object_by_hash.items()
                          if item["kind"] == "pack"}
    return {"counts": counts, "object_members": object_members,
            "chunked_files": chunked_files, "chunked_logical_bytes": chunked_logical_bytes,
            "chunk_only_objects": chunk_objects - direct_blob_objects}


@contextlib.contextmanager
def _native_pack_source(path: Path, item: Mapping[str, Any], *, enabled: bool,
                        prefer_memory: bool = True):
    """Read one fully verified immutable pack with bounded optional staging.

    Only the guarded canonical-D callers enable this adapter. Every byte is
    hashed against its object ID before ZIP access; no tree/current/lease is
    hydrated. Small packs may use bounded memory; when SSD reserve is not
    available, verify the complete D object before portable ZIP seeks instead.
    Anonymous scratch is removed when this read context closes.
    """
    if type(prefer_memory) is not bool:
        raise SnapshotError("pack read preference requires an explicit boolean")
    if not enabled:
        yield path
        return
    from stockagent.data_sync.windows_cold_io import binary_reader, hash_file
    before = path.lstat()
    expected_bytes = int(item["bytes"])
    if (not stat.S_ISREG(before.st_mode) or path.is_symlink() or before.st_size != expected_bytes):
        raise SnapshotError("native pack input is redirected or its size changed")
    fingerprint = (_source_stat(before), before.st_nlink)
    # Complete recovery of a real 876 kB / 67-member legacy pack measured
    # medians of 0.888s in memory, 1.078s via DrvFs ZIP seeks, and 3.093s via
    # native SSD scratch. Keep this choice bounded to one MiB / 128 members;
    # unknown counts, larger packs and larger stored subsets stay native.
    # See docs/vastai1t_storage_cleanup_2026-10-06.md for the interleaved runs.
    stored_members = int(item.get('stored_file_count', item.get('file_count', 129)))
    if prefer_memory and expected_bytes <= 1024**2 and 1 <= stored_members <= 128:
        with path.open('rb') as source:
            payload = source.read(expected_bytes + 1)
        if (len(payload) != expected_bytes or hashlib.sha256(payload).hexdigest() != item['sha256']
                or (_source_stat(path.lstat()), path.lstat().st_nlink) != fingerprint):
            raise SnapshotError('native pack checksum or immutable source signature differs')
        with io.BytesIO(payload) as source:
            yield source
        if (_source_stat(path.lstat()), path.lstat().st_nlink) != fingerprint:
            raise SnapshotError('native pack changed during reconstruction')
        return
    scratch_root = Path("/var/lib")
    if shutil.disk_usage(scratch_root).free < expected_bytes + 32 * 1024**3:
        # SSD staging is an optimization, not a prerequisite for recovery/GC.
        # With a full SSD use the portable seekable D reader, after a complete
        # native SHA pass, and retain the same immutable-generation fences.
        # Never allocate scratch, waive its reserve, or expose unverified ZIP.
        if (hash_file(path) != item["sha256"]
                or (_source_stat(path.lstat()), path.lstat().st_nlink) != fingerprint):
            raise SnapshotError("native pack checksum or immutable source signature differs")
        yield path
        if (_source_stat(path.lstat()), path.lstat().st_nlink) != fingerprint:
            raise SnapshotError("native pack changed during reconstruction")
        return
    from stockagent.data_sync.windows_cold_io import admit_workspace
    admit_workspace(scratch_root, expected_bytes + 32 * 1024**3)
    with tempfile.TemporaryFile(dir=scratch_root) as scratch:
        digest, total = hashlib.sha256(), 0
        with binary_reader(path) as source:
            while block := source.read(8 * 1024 * 1024):
                total += len(block)
                if total > expected_bytes:
                    raise SnapshotError("native pack changed while reading")
                digest.update(block)
                scratch.write(block)
        if (total != expected_bytes or digest.hexdigest() != item["sha256"]
                or (_source_stat(path.lstat()), path.lstat().st_nlink) != fingerprint):
            raise SnapshotError("native pack checksum or immutable source signature differs")
        scratch.flush()
        os.fsync(scratch.fileno())
        scratch.seek(0)
        yield scratch
        if (_source_stat(path.lstat()), path.lstat().st_nlink) != fingerprint:
            raise SnapshotError("native pack changed during reconstruction")


MAX_NATIVE_HASH_MEMBER_BYTES = 8 * 1024**3
MAX_NATIVE_HASH_BATCH_BYTES = 16 * 1024**3


def _native_blob_hash_batches(members):
    batch, total = [], 0
    for path, count in members:
        if not 0 <= count <= MAX_NATIVE_HASH_MEMBER_BYTES:
            continue  # the caller keeps its full streaming SHA path
        if batch and (len(batch) == 16 or total + count > MAX_NATIVE_HASH_BATCH_BYTES):
            yield batch
            batch, total = [], 0
        batch.append(path)
        total += count
    if batch:
        yield batch


def verify_packed_snapshot(
    sync_root: Path,
    resolved: ResolvedSnapshot,
    *,
    materialized_path: Path | None = None,
    reconstruct_paths: Iterable[str] | None = None,
    reconstruct_all: bool = False,
    d_primary_native_blob_reads: bool = False,
    d_primary_native_pack_reads: bool = False,
    d_primary_native_chunk_reads: bool = False,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    with _verified_packed_inventory(sync_root, resolved, materialized_path=materialized_path,
            reconstruct_paths=reconstruct_paths, reconstruct_all=reconstruct_all,
            d_primary_native_blob_reads=d_primary_native_blob_reads,
            d_primary_native_pack_reads=d_primary_native_pack_reads,
            d_primary_native_chunk_reads=d_primary_native_chunk_reads, progress=progress) as (_, proof):
        return proof


@contextlib.contextmanager
def _verified_packed_inventory(sync_root, resolved, *, materialized_path=None,
                                reconstruct_paths=None, reconstruct_all=False,
                                d_primary_native_blob_reads=False, d_primary_native_pack_reads=False,
                                progress=None, d_primary_native_chunk_reads=False):
    sync_root = sync_root.resolve()
    if type(reconstruct_all) is not bool or (reconstruct_all and reconstruct_paths is not None):
        raise SnapshotError("choose either all-file or explicit-path reconstruction")
    if (type(d_primary_native_blob_reads) is not bool or type(d_primary_native_pack_reads) is not bool
            or type(d_primary_native_chunk_reads) is not bool):
        raise SnapshotError('native D verification requires an explicit boolean')
    if (d_primary_native_blob_reads or d_primary_native_chunk_reads) and sync_root != Path('/srv/stockagent-packed'):
        raise SnapshotError('native D verification is restricted to canonical D authority')
    if d_primary_native_pack_reads or d_primary_native_chunk_reads:
        _check_native_pack_read_authority(sync_root)
    manifest = resolved.manifest
    _validate_manifest(manifest)
    if sha256_file(resolved.manifest_path) != resolved.manifest_sha256:
        raise SnapshotError(
            f"manifest changed after resolution: {resolved.manifest_path}"
        )
    _validate_object_presence(sync_root, manifest)
    with _open_inventory(sync_root, manifest) as entries:
        proof = _verify_packed_inventory(sync_root, resolved, entries,
            materialized_path=materialized_path, reconstruct_paths=reconstruct_paths,
            reconstruct_all=reconstruct_all, d_primary_native_blob_reads=d_primary_native_blob_reads,
            d_primary_native_pack_reads=d_primary_native_pack_reads,
            d_primary_native_chunk_reads=d_primary_native_chunk_reads, progress=progress)
        yield entries, proof


def _check_native_pack_read_authority(sync_root: Path) -> None:
    """The same D authority, including its already enrolled local-only history.

    This enables only verified pack reads into anonymous bounded SSD scratch;
    private publication, export, NAS and source-retirement rights do not change.
    Reuse the migration owner's exact marker/permission/ignore checks rather
    than treating every directory below .local-state as an authorized history.
    """
    primary = Path('/srv/stockagent-packed')
    if sync_root == primary:
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        _check_d_primary_mount(primary)
        return
    if sync_root != primary / '.local-state/private-history':
        raise SnapshotError('native D verification is restricted to canonical D authority')
    from stockagent.data_sync.dataset_migration import private_history_root
    policy = {"private_history": {"enabled": True, "relative_root": ".local-state/private-history",
        "shared_export": False, "nas_export": False, "source_retirement": False}}
    if private_history_root(primary, policy, initialize=False) != sync_root:
        raise SnapshotError('native private pack read authority differs')


def _verify_packed_inventory(sync_root, resolved, entries, *, materialized_path,
                             reconstruct_paths, reconstruct_all,
                             d_primary_native_blob_reads, d_primary_native_pack_reads, progress,
                             d_primary_native_chunk_reads=False):
    manifest = resolved.manifest
    inventory_result = _validate_inventory(manifest, entries)
    object_count = len(manifest['archive']['objects'])
    object_bytes = sum(int(item['bytes']) for item in manifest['archive']['objects'])
    if progress is not None:
        progress({'stage': 'verify-cold-objects', 'completed_items': 0,
                  'total_items': object_count, 'completed_bytes': 0, 'total_bytes': object_bytes})
    selected_paths = set(reconstruct_paths) if reconstruct_paths is not None else None
    file_rows = (
        {row["path"]: row for row in entries if row["kind"] == "file" and row["path"] in selected_paths}
        if selected_paths is not None else {}
    )
    if selected_paths is not None and not selected_paths.issubset(file_rows):
        raise SnapshotError(
            "reconstruction request contains a non-file or unknown inventory path"
        )
    selected_by_object: dict[str, list[dict[str, Any]]] = {}
    chunked_rows = entries.chunked_rows() if selected_paths is None else []
    for relative in sorted(selected_paths or ()):
        row = file_rows[relative]
        if row["storage"]["kind"] == "chunks":
            chunked_rows.append(row)
        else:
            selected_by_object.setdefault(row["storage"]["object_sha256"], []).append(row)
    native_blob_digests: dict[Path, str] = {}
    native_blob_signatures = {}
    # Full original reconstruction already reads and hashes every chunk. Fuse
    # that read with its object proof instead of doing another cold-volume pass.
    # Objects also serving a direct blob retain the existing first-pass proof.
    deferred_objects = inventory_result['chunk_only_objects'] if reconstruct_all else set()
    pending_objects = set(deferred_objects)
    deferred_signatures = {}
    if d_primary_native_blob_reads:
        from stockagent.data_sync.windows_cold_io import hash_many, _stable
        blob_members = [(_path_under(sync_root, str(item['relpath']), 'object relpath'), int(item['bytes']))
                        for item in manifest['archive']['objects']
                        if item['kind'] == 'blob' and item['sha256'] not in deferred_objects]
        blob_paths = [path for path, count in blob_members if count <= MAX_NATIVE_HASH_MEMBER_BYTES]
        native_blob_signatures = {path: path.lstat() for path in blob_paths}
        hashed_bytes = 0
        hash_bytes = sum(count for _, count in blob_members if count <= MAX_NATIVE_HASH_MEMBER_BYTES)
        # Keep every full object hash and native before/after signature gate;
        # amortize the Windows process start across a bounded set of objects.
        for batch in _native_blob_hash_batches(blob_members):
            native_blob_digests.update(hash_many(batch))
            hashed_bytes += sum(native_blob_signatures[path].st_size for path in batch)
            if progress is not None:
                progress({'stage': 'hash-cold-blobs', 'completed_items': len(native_blob_digests),
                          'total_items': len(blob_paths), 'completed_bytes': hashed_bytes,
                          'total_bytes': hash_bytes})
    reconstructed_files = 0
    reconstructed_bytes = 0
    verified_bytes = 0
    verified_objects = 0
    for item in manifest["archive"]["objects"]:
        path = _path_under(sync_root, str(item["relpath"]), "object relpath")
        if item['sha256'] in deferred_objects:
            deferred_signatures[path] = path.lstat()
            continue  # Not counted as verified until _iter_chunked_file accepts it.
        # Packs are already size-checked above.  For a bounded pack, use the
        # same immutable bytes for SHA-256 and ZIP CRC instead of reading the
        # cold volume twice.  Large packs and blobs keep the streaming path.
        pack_bytes = (
            path.read_bytes()
            if item["kind"] == "pack"
            and not d_primary_native_pack_reads
            and int(item["bytes"]) <= MAX_IN_MEMORY_PACK_VERIFY_BYTES
            else None
        )
        if d_primary_native_pack_reads and item['kind'] == 'pack':
            # _native_pack_source performs the complete object SHA before ZIP
            # access, under before/after immutable source signatures.
            actual = item['sha256']
        elif path in native_blob_digests and item['kind'] == 'blob':
            actual = native_blob_digests[path]
            if not _stable(native_blob_signatures[path], path.lstat()):
                raise SnapshotError('native cold blob changed after full batch hashing')
        else:
            actual = (
                hashlib.sha256(pack_bytes).hexdigest()
                if pack_bytes is not None else sha256_file(path)
            )
        if actual != item["sha256"]:
            raise SnapshotError(
                f"packed object checksum mismatch: expected {item['sha256']}, got {actual}"
            )
        verified_bytes += int(item["bytes"])
        verified_objects += 1
        if item["kind"] == "pack":
            expected_names = list(inventory_result["object_members"][str(item["sha256"])])
            try:
                source = io.BytesIO(pack_bytes) if pack_bytes is not None else path
                with _native_pack_source(path, item, enabled=d_primary_native_pack_reads) as native_source, \
                        zipfile.ZipFile(native_source if d_primary_native_pack_reads else source, mode="r") as archive:
                    names = archive.namelist()
                    if item.get("member_selection") == "subset":
                        if not _is_ordered_subset(expected_names, names):
                            raise SnapshotError(f"ZIP members do not contain inventory subset: {path}")
                    elif names != expected_names:
                        raise SnapshotError(f"ZIP member list differs from inventory: {path}")
                    if reconstruct_all:
                        # One decompression pass provides both ZIP CRC for
                        # EVERY stored member (including old subset members)
                        # and the independent full-file SHA for every current
                        # original. Do not run testzip() then decode again.
                        current = iter(entries.object_rows(str(item["sha256"])))
                        row = next(current, None)
                        for name in names:
                            selected = row is not None and row["path"] == name
                            digest, decoded_size = hashlib.sha256(), 0
                            with archive.open(name, mode="r") as stream:
                                while block := stream.read(8 * 1024 * 1024):
                                    if selected:
                                        digest.update(block)
                                        decoded_size += len(block)
                            if selected:
                                if decoded_size != row["size"] or digest.hexdigest() != row["sha256"]:
                                    raise SnapshotError(f"exact cold reconstruction failed: {row['path']}")
                                reconstructed_files += 1
                                reconstructed_bytes += decoded_size
                                row = next(current, None)
                        if row is not None:
                            raise SnapshotError("not every original pack member was reconstructed")
                        bad_member = None  # CRC already checked by ZipExtFile
                    else:
                        bad_member = archive.testzip()
                    # Reuse these already-hashed immutable pack bytes, rather
                    # than reopen a DrvFs/HDD pack per small cached file.
                    for row in selected_by_object.get(str(item["sha256"]), ()):
                        digest = hashlib.sha256()
                        decoded_size = 0
                        with archive.open(
                            str(row["storage"]["member"]), mode="r"
                        ) as stream:
                            while chunk := stream.read(8 * 1024 * 1024):
                                decoded_size += len(chunk)
                                digest.update(chunk)
                        if (
                            decoded_size != int(row["size"])
                            or digest.hexdigest() != row["sha256"]
                        ):
                            raise SnapshotError(
                                f"exact cold reconstruction failed: {row['path']}"
                            )
                        reconstructed_files += 1
                        reconstructed_bytes += int(row["size"])
            except (OSError, KeyError, zipfile.BadZipFile) as exc:
                raise SnapshotError(f"invalid ZIP pack {path}: {exc}") from exc
            if bad_member is not None:
                raise SnapshotError(f"ZIP CRC check failed for {bad_member} in {path}")
        else:
            rows = (entries.object_rows(str(item["sha256"])) if reconstruct_all
                    else selected_by_object.get(str(item["sha256"]), ()))
            for row in rows:
                if actual != row["sha256"] or int(item["bytes"]) != int(row["size"]):
                    raise SnapshotError(
                        f"exact cold blob reconstruction failed: {row['path']}"
                    )
                reconstructed_files += 1
                reconstructed_bytes += int(row["size"])
        if progress is not None:
            progress({'stage': 'verify-cold-objects', 'completed_items': verified_objects,
                      'total_items': object_count, 'completed_bytes': verified_bytes,
                      'total_bytes': object_bytes})
    objects = {str(item["sha256"]): item for item in manifest["archive"]["objects"]}
    chunked_count = (len(chunked_rows) if selected_paths is not None else inventory_result["chunked_files"])
    reconstruction_total = (int(manifest["source"]["logical_bytes"]) if reconstruct_all else
                            sum(int(file_rows[name]["size"]) for name in selected_paths)
                            if selected_paths is not None else inventory_result["chunked_logical_bytes"])
    reconstruction_items = (int(manifest["source"]["files"]) if reconstruct_all else
                            len(selected_paths) if selected_paths is not None else chunked_count)
    def verified_extent(digest):
        nonlocal verified_bytes, verified_objects
        if digest in pending_objects:
            pending_objects.remove(digest)
            verified_objects += 1
            verified_bytes += int(objects[digest]['bytes'])
    if progress is not None:
        progress({'stage': 'reconstruct-original-files', 'completed_items': reconstructed_files,
                  'total_items': reconstruction_items, 'completed_bytes': reconstructed_bytes,
                  'total_bytes': reconstruction_total})
    for row in chunked_rows:
        complete = hashlib.sha256()
        decoded_size = 0
        native_chunks = d_primary_native_blob_reads or (
            d_primary_native_chunk_reads and int(row['size']) >= MIN_NATIVE_CHUNK_STREAM_BYTES)
        for block in _iter_chunked_file(sync_root, objects, row, native=native_chunks,
                                       **({'verified_extent': verified_extent} if deferred_objects else {})):
            decoded_size += len(block)
            complete.update(block)
            if progress is not None:
                progress({'stage': 'reconstruct-original-files', 'completed_items': reconstructed_files,
                          'total_items': reconstruction_items,
                          'completed_bytes': reconstructed_bytes + decoded_size,
                          'native_chunk_batch_reads': native_chunks,
                          'total_bytes': reconstruction_total,
                          'verified_objects': verified_objects, 'total_objects': object_count,
                          'verified_object_bytes': verified_bytes, 'total_object_bytes': object_bytes})
        if decoded_size != row["size"] or complete.hexdigest() != row["sha256"]:
            raise SnapshotError(f"exact cold chunk reconstruction failed: {row['path']}")
        reconstructed_files += 1
        reconstructed_bytes += decoded_size
        if progress is not None:
            progress({'stage': 'reconstruct-original-files', 'completed_items': reconstructed_files,
                      'total_items': reconstruction_items, 'completed_bytes': reconstructed_bytes,
                      'total_bytes': reconstruction_total,
                      'verified_objects': verified_objects, 'total_objects': object_count,
                      'verified_object_bytes': verified_bytes, 'total_object_bytes': object_bytes})
    if pending_objects or verified_objects != object_count or verified_bytes != object_bytes:
        raise SnapshotError('not every cold object was independently verified')
    from stockagent.data_sync.windows_cold_io import _stable
    if any(not _stable(before, path.lstat()) for path, before in deferred_signatures.items()):
        raise SnapshotError('cold chunk changed during fused original reconstruction')
    if deferred_objects and progress is not None:
        progress({'stage': 'verify-cold-objects', 'completed_items': verified_objects,
                  'total_items': object_count, 'completed_bytes': verified_bytes, 'total_bytes': object_bytes})
    result: dict[str, Any] = {
        "snapshot_id": manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256,
        "inventory_sha256": manifest["archive"]["inventory"]["sha256"],
        "objects": len(manifest["archive"]["objects"]),
        "verified_object_bytes": verified_bytes,
        "materialized_verified": False,
        "chunked_files_verified": chunked_count,
    }
    if selected_paths is not None or reconstruct_all:
        if reconstructed_files != reconstruction_items or reconstructed_bytes != reconstruction_total:
            raise SnapshotError("not every requested file was reconstructed")
        result["independently_reconstructed_files"] = reconstructed_files
        result["independently_reconstructed_bytes"] = reconstructed_bytes
    if deferred_objects:
        result['chunk_object_hash_fusion'] = {
            'contract': 'full_chunk_sha_and_original_sha_single_read_v1',
            'objects': len(deferred_objects),
            'redundant_cold_read_bytes_avoided': sum(int(objects[digest]['bytes']) for digest in deferred_objects),
        }
    if materialized_path is not None:
        _verify_materialized(materialized_path, manifest, entries)
        result["materialized_verified"] = True
    if any(not _stable(before, path.lstat()) for path, before in native_blob_signatures.items()):
        raise SnapshotError('native cold blob changed during release verification')
    return result


def _iter_chunked_file(
    sync_root: Path,
    objects: Mapping[str, Mapping[str, Any]],
    row: Mapping[str, Any],
    *,
    native: bool = False,
    verified_extent: Callable[[str], None] | None = None,
):
    """Yield bounded exact bytes while verifying each extent, including repeats."""
    extents = iter(_storage_extents(row["storage"], int(row["size"])))
    while batch := list(itertools.islice(extents, 16)):
        paths = [_path_under(sync_root, str(objects[digest]["relpath"]), "chunk object relpath")
                 for digest, _ in batch]
        if native and len(batch) > 1:
            from stockagent.data_sync.windows_cold_io import binary_reader_many, windows_path
            if all(windows_path(path) is not None for path in paths):
                with binary_reader_many([(path, size) for path, (_, size) in zip(paths, batch, strict=True)]) as stream:
                    for path, (digest, expected_size) in zip(paths, batch, strict=True):
                        chunk_hash = hashlib.sha256()
                        remaining = expected_size
                        while remaining:
                            block = stream.read(min(8 * 1024 * 1024, remaining))
                            if not block:
                                raise SnapshotError(f"truncated chunk object: {path}")
                            remaining -= len(block)
                            chunk_hash.update(block)
                            yield block
                        if chunk_hash.hexdigest() != digest:
                            raise SnapshotError(f"chunk object checksum/size mismatch: {path}")
                    if stream.read(1):
                        raise SnapshotError('chunk batch has excess bytes')
                # Do not count an extent until all batch lifecycle guards pass.
                if verified_extent is not None:
                    for digest, _ in batch:
                        verified_extent(digest)
                continue
        for path, (digest, expected_size) in zip(paths, batch, strict=True):
            yield from _iter_single_chunk(path, digest, expected_size, native=native)
            if verified_extent is not None:
                verified_extent(digest)


def _iter_single_chunk(path: Path, digest: str, expected_size: int, *, native: bool):
    """Portable/single-member fallback; retain the exact per-object EOF check."""
    if native:
        from stockagent.data_sync.windows_cold_io import binary_reader
        reader = binary_reader(path)
    else:
        reader = path.open("rb")
    chunk_hash = hashlib.sha256()
    remaining = expected_size
    with reader as stream:
        while remaining:
            block = stream.read(min(8 * 1024 * 1024, remaining))
            if not block:
                raise SnapshotError(f"truncated chunk object: {path}")
            remaining -= len(block)
            chunk_hash.update(block)
            yield block
        if stream.read(1) or chunk_hash.hexdigest() != digest:
            raise SnapshotError(f"chunk object checksum/size mismatch: {path}")


class _ChunkedFileReader:
    """A bounded read adapter for the common verified extraction engine."""

    def __init__(self, blocks):
        self.blocks = iter(blocks)
        self.pending = b""

    def read(self, size: int) -> bytes:
        if not 1 <= size <= 8 * 1024 * 1024:
            raise SnapshotError("chunk reconstruction requires bounded reads")
        result = bytearray()
        while len(result) < size:
            if not self.pending:
                self.pending = next(self.blocks, b"")
                if not self.pending:
                    break
            take = min(size - len(result), len(self.pending))
            result.extend(self.pending[:take])
            self.pending = self.pending[take:]
        return bytes(result)

    def close(self):
        self.blocks.close()


class _FileDurabilityQueue:
    """Bounded overlapping file barriers; publication still waits for ALL.

    Each job owns one duplicated descriptor to the already flushed, chmod'ed
    original inode. No path reopen, omitted fsync or premature READY is used.
    """
    def __init__(self, workers):
        if type(workers) is not int or not 0 <= workers <= 8:
            raise SnapshotError("file durability concurrency must be between zero and eight")
        self.workers = workers
        self.pending = deque()
        self.pool = None

    def __enter__(self):
        if self.workers:
            self.pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="packed-fsync")
        return self

    @staticmethod
    def _sync_owned(descriptor):
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def submit(self, descriptor):
        if self.pool is None:
            os.fsync(descriptor)
            return
        if len(self.pending) >= self.workers * 2:
            self.pending.popleft().result()
        duplicate = os.dup(descriptor)
        try:
            future = self.pool.submit(self._sync_owned, duplicate)
        except BaseException:
            os.close(duplicate)
            raise
        self.pending.append(future)

    def __exit__(self, exc_type, *_):
        failure = None
        try:
            while self.pending:
                try:
                    self.pending.popleft().result()
                except BaseException as error:
                    if failure is None:
                        failure = error
        finally:
            if self.pool is not None:
                self.pool.shutdown(wait=True)
        if exc_type is None and failure is not None:
            raise failure


def _copy_member_and_verify(
    input_stream: Any,
    destination: Path,
    *,
    expected_size: int,
    expected_sha256: str,
    mode: int,
    durable: bool = True,
    durability_queue: _FileDurabilityQueue | None = None,
) -> None:
    if type(durable) is not bool:
        raise SnapshotError("member durability requires an explicit boolean")
    digest = hashlib.sha256()
    copied = 0
    with destination.open("xb") as output_stream:
        # A source that grows after verification must not exceed the caller's
        # admitted growth. Probe at most one extra byte, never write it.
        while block := input_stream.read(min(8 * 1024 * 1024, expected_size - copied + 1)):
            copied += len(block)
            if copied > expected_size:
                raise SnapshotError(f"extracted file exceeds admitted size: {destination}")
            digest.update(block)
            output_stream.write(block)
        output_stream.flush()
        if copied != expected_size or digest.hexdigest() != expected_sha256:
            raise SnapshotError(f"extracted file checksum mismatch: {destination}")
        # Persist bytes and their final mode with ONE barrier, not a second
        # fsync after path-based chmod. Failed members are never committed.
        os.fchmod(output_stream.fileno(), mode)
        if durable:
            if durability_queue is None:
                os.fsync(output_stream.fileno())
            else:
                durability_queue.submit(output_stream.fileno())


def _materialize_regular_files(
    sync_root: Path,
    manifest: Mapping[str, Any],
    entries: Iterable[dict[str, Any]],
    destination_for: Callable[[dict[str, Any]], Path],
    *,
    d_primary_native_blob_reads: bool,
    d_primary_native_pack_reads: bool,
    durable: bool,
    on_copy: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    workers = (MATERIALIZE_FSYNC_WORKERS
               if durable and int(manifest["source"]["files"]) >= MIN_PARALLEL_FSYNC_FILES else 0)
    with _FileDurabilityQueue(workers) as barriers:
        _materialize_indexed_files(sync_root, manifest, entries, destination_for,
            d_primary_native_blob_reads=d_primary_native_blob_reads,
            d_primary_native_pack_reads=d_primary_native_pack_reads,
            durable=durable, on_copy=on_copy, durability_queue=barriers)


def _materialize_indexed_files(sync_root, manifest, entries, destination_for, *,
                                d_primary_native_blob_reads, d_primary_native_pack_reads,
                                durable, on_copy, durability_queue):
    """One byte-copy engine for complete trees and bounded flat staging.

    Callers first validate the entire fixed release and prepare their own path
    mapping. Every logical file still receives its own full streamed checksum;
    identical blobs may use an already verified private copy as their reader.
    """
    objects = {str(item["sha256"]): item for item in manifest["archive"]["objects"]}
    blobs: dict[str, list[dict[str, Any]]] = {}
    packs: dict[str, list[dict[str, Any]]] = {}
    for row in entries.chunked_rows() if isinstance(entries, MetadataRows) else entries:
        if row["kind"] == "file":
            if row["storage"]["kind"] == "chunks":
                with contextlib.closing(_ChunkedFileReader(_iter_chunked_file(
                    sync_root, objects, row, native=d_primary_native_blob_reads
                ))) as input_stream:
                    _copy_member_and_verify(input_stream, destination_for(row),
                                            expected_size=int(row["size"]),
                                            expected_sha256=str(row["sha256"]), mode=int(row["mode"]),
                                            durable=durable, durability_queue=durability_queue)
                if on_copy is not None:
                    on_copy(row)
                continue
            if not isinstance(entries, MetadataRows):
                groups = blobs if row["storage"]["kind"] == "blob" else packs
                groups.setdefault(str(row["storage"]["object_sha256"]), []).append(row)
    # The same index used for SHA/CRC verification also groups extraction;
    # do not construct another complete file-row dictionary just to open each
    # pack once. Legacy bounded list callers retain their existing path.
    if isinstance(entries, MetadataRows):
        blobs = {digest: entries.object_rows(digest) for digest, item in objects.items() if item["kind"] == "blob"}
        packs = {digest: entries.object_rows(digest) for digest, item in objects.items() if item["kind"] == "pack"}
    for object_sha, rows in blobs.items():
        object_path = _path_under(sync_root, str(objects[object_sha]["relpath"]), "object relpath")
        verified_source = None
        for row in rows:
            path = destination_for(row)
            if verified_source is not None:
                reader = verified_source.open("rb")
            elif d_primary_native_blob_reads:
                from stockagent.data_sync.windows_cold_io import binary_reader
                reader = binary_reader(object_path)
            else:
                reader = object_path.open("rb")
            with reader as input_stream:
                _copy_member_and_verify(input_stream, path, expected_size=int(row["size"]),
                                        expected_sha256=str(row["sha256"]), mode=int(row["mode"]),
                                        durable=durable, durability_queue=durability_queue)
            if on_copy is not None:
                on_copy(row)
            if verified_source is None and int(row["mode"]) & stat.S_IRUSR:
                verified_source = path
    for object_sha, rows in packs.items():
        item = objects[object_sha]
        object_path = _path_under(sync_root, str(item["relpath"]), "object relpath")
        with _native_pack_source(object_path, item, enabled=d_primary_native_pack_reads) as source, \
                zipfile.ZipFile(source, mode="r") as archive:
            for row in rows:
                with archive.open(str(row["storage"]["member"]), mode="r") as input_stream:
                    _copy_member_and_verify(input_stream, destination_for(row),
                                            expected_size=int(row["size"]),
                                            expected_sha256=str(row["sha256"]), mode=int(row["mode"]),
                                            durable=durable, durability_queue=durability_queue)
                if on_copy is not None:
                    on_copy(row)


def _verify_materialized(
    materialized_path: Path,
    manifest: Mapping[str, Any],
    entries: Iterable[dict[str, Any]],
) -> None:
    for row in entries:
        path = materialized_path.joinpath(*PurePosixPath(row["path"]).parts)
        kind = row["kind"]
        if kind == "directory" and not path.is_dir():
            raise SnapshotError(f"materialized directory is missing: {path}")
        if kind == "symlink":
            if not path.is_symlink() or os.readlink(path) != row["target"]:
                raise SnapshotError(f"materialized symlink differs: {path}")
        if kind == "file":
            if not path.is_file() or path.stat().st_size != int(row["size"]):
                raise SnapshotError(f"materialized file differs: {path}")
            if sha256_file(path) != row["sha256"]:
                raise SnapshotError(f"materialized file checksum mismatch: {path}")
    inventory = scan_tree(materialized_path)
    expected = manifest["source"]["portable_fingerprint_sha256"]
    if inventory["portable_fingerprint_sha256"] != expected:
        raise SnapshotError(
            f"materialized fingerprint mismatch: expected {expected}, "
            f"got {inventory['portable_fingerprint_sha256']}"
        )


def _subtree_entries(
    entries: list[dict[str, Any]], subtree: PurePosixPath
) -> list[dict[str, Any]]:
    prefix = subtree.as_posix()
    selected = [
        row
        for row in entries
        if row["path"] == prefix or row["path"].startswith(f"{prefix}/")
    ]
    if not selected:
        raise SnapshotError(f"packed snapshot subtree is missing: {prefix}")
    root = selected[0]
    if root["path"] != prefix or root["kind"] != "directory":
        raise SnapshotError(f"packed snapshot subtree is not a directory: {prefix}")
    return selected


def _subtree_destination(
    root: Path, subtree: PurePosixPath, row: Mapping[str, Any]
) -> Path:
    relative = PurePosixPath(str(row["path"])).relative_to(subtree)
    return root.joinpath(*relative.parts)


def _verify_materialized_subtree(
    materialized_path: Path,
    subtree: PurePosixPath,
    entries: list[dict[str, Any]],
) -> dict[str, int]:
    expected_counts = {"files": 0, "directories": 0, "symlinks": 0, "logical_bytes": 0}
    for row in entries:
        path = _subtree_destination(materialized_path, subtree, row)
        kind = row["kind"]
        if kind == "directory":
            if not path.is_dir():
                raise SnapshotError(
                    f"materialized subtree directory is missing: {path}"
                )
            if row["path"] != subtree.as_posix():
                expected_counts["directories"] += 1
        elif kind == "symlink":
            if not path.is_symlink() or os.readlink(path) != row["target"]:
                raise SnapshotError(f"materialized subtree symlink differs: {path}")
            expected_counts["symlinks"] += 1
        elif kind == "file":
            if not path.is_file() or path.stat().st_size != int(row["size"]):
                raise SnapshotError(f"materialized subtree file differs: {path}")
            if sha256_file(path) != row["sha256"]:
                raise SnapshotError(
                    f"materialized subtree file checksum mismatch: {path}"
                )
            expected_counts["files"] += 1
            expected_counts["logical_bytes"] += int(row["size"])
        else:  # pragma: no cover - full inventory validation owns this guard.
            raise SnapshotError(f"invalid materialized subtree kind: {kind!r}")
    actual = scan_tree(materialized_path)
    for key, expected in expected_counts.items():
        if int(actual[key]) != expected:
            raise SnapshotError(
                f"materialized subtree {key} mismatch: expected {expected}, "
                f"got {actual[key]}"
            )
    return expected_counts


def fetch_packed_subtree(
    sync_root: Path,
    materialized_root: Path,
    resolved: ResolvedSnapshot,
    subtree: str,
) -> Path:
    """Atomically materialize one verified directory from a packed snapshot.

    Large migration snapshots can contain many unrelated datasets.  This keeps
    the same manifest, inventory, object, and per-file checksum authority as a
    full fetch while avoiding extraction of unrelated bytes.
    """
    from stockagent.data_sync.cache_capacity import reserve_cache_write

    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if _paths_overlap(sync_root, materialized_root):
        raise SnapshotError("materialized root must be outside the packed sync root")
    relative_subtree = _safe_relative_path(subtree, "subtree")
    manifest = resolved.manifest
    _validate_manifest(manifest)
    if sha256_file(resolved.manifest_path) != resolved.manifest_sha256:
        raise SnapshotError(
            f"manifest changed after resolution: {resolved.manifest_path}"
        )
    _validate_object_presence(sync_root, manifest)
    entries = _load_inventory(sync_root, manifest)
    _validate_inventory(manifest, entries)
    selected = _subtree_entries(entries, relative_subtree)

    dataset = validate_slug(str(manifest["dataset"]), "dataset")
    snapshot_id = validate_slug(str(manifest["snapshot_id"]), "snapshot_id")
    target = materialized_root / dataset / snapshot_id
    target = target.joinpath(*relative_subtree.parts)
    ready_name = hashlib.sha256(relative_subtree.as_posix().encode()).hexdigest()[:16]
    ready_path = target.parent / f".{target.name}.{ready_name}.READY.json"
    lock_path = (
        materialized_root
        / ".locks"
        / f"fetch-{dataset}-{snapshot_id}-subtree-{ready_name}.lock"
    )
    growth = (sum(int(row["size"]) + 8192 for row in selected if row["kind"] == "file")
              + len(selected) * 4096 + 1024**2)
    with _exclusive_lock(lock_path), reserve_cache_write(
            materialized_root, key=f"{resolved.manifest_sha256}:{ready_name}",
            maximum_growth_bytes=growth, family="materialized", enabled=not target.exists()):
        if target.exists():
            counts = _verify_materialized_subtree(target, relative_subtree, selected)
            if not ready_path.exists():
                atomic_write_json(
                    ready_path,
                    {
                        "snapshot_id": snapshot_id,
                        "manifest_sha256": resolved.manifest_sha256,
                        "subtree": relative_subtree.as_posix(),
                        "counts": counts,
                        "verified_at": _utc_iso_from_ns(time.time_ns()),
                    },
                )
            return target

        target.parent.mkdir(parents=True, exist_ok=True)
        staging = target.parent / f".{target.name}.partial.{uuid.uuid4().hex}"
        staging.mkdir()
        try:
            object_by_hash = {
                str(item["sha256"]): item for item in manifest["archive"]["objects"]
            }
            selected_object_hashes = {
                digest
                for row in selected
                if row["kind"] == "file"
                for digest, _ in _storage_extents(row["storage"], int(row["size"]))
            }
            for object_sha in sorted(selected_object_hashes):
                item = object_by_hash[object_sha]
                object_path = _path_under(
                    sync_root, str(item["relpath"]), "object relpath"
                )
                if sha256_file(object_path) != object_sha:
                    raise SnapshotError(
                        f"packed object checksum mismatch: expected {object_sha}, "
                        f"got corrupt object {object_path}"
                    )
            for row in selected:
                if row["kind"] == "directory":
                    path = _subtree_destination(staging, relative_subtree, row)
                    path.mkdir(parents=True, exist_ok=True)
                    path.chmod(int(row["mode"]))
            for row in selected:
                path = _subtree_destination(staging, relative_subtree, row)
                path.parent.mkdir(parents=True, exist_ok=True)
                if row["kind"] == "symlink":
                    path.symlink_to(str(row["target"]))
            _materialize_regular_files(sync_root, manifest, selected,
                lambda row: _subtree_destination(staging, relative_subtree, row),
                d_primary_native_blob_reads=False, d_primary_native_pack_reads=False, durable=True)
            counts = _verify_materialized_subtree(staging, relative_subtree, selected)
            os.replace(staging, target)
            _fsync_directory(target.parent)
        except Exception:
            # Keep the partial tree for diagnosis; it is never treated as ready.
            raise
        atomic_write_json(
            ready_path,
            {
                "snapshot_id": snapshot_id,
                "manifest_sha256": resolved.manifest_sha256,
                "inventory_sha256": manifest["archive"]["inventory"]["sha256"],
                "subtree": relative_subtree.as_posix(),
                "counts": counts,
                "verified_at": _utc_iso_from_ns(time.time_ns()),
            },
        )
        return target


def fetch_packed_snapshot(
    sync_root: Path,
    materialized_root: Path,
    resolved: ResolvedSnapshot,
    *,
    d_primary_native_blob_reads: bool = False,
    d_primary_native_pack_reads: bool = False,
    verification_only: bool = False,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> Path:
    from stockagent.data_sync.cache_capacity import reserve_cache_write

    if type(verification_only) is not bool:
        raise SnapshotError("private verification reconstruction requires an explicit boolean")
    if verification_only:
        parent = materialized_root.absolute()
        before = parent.lstat()
        if (parent.resolve() != parent or not stat.S_ISDIR(before.st_mode)
                or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o700
                or any(parent.iterdir())):
            raise SnapshotError("verification reconstruction requires a fresh private scratch directory")
    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if _paths_overlap(sync_root, materialized_root):
        raise SnapshotError("materialized root must be outside the packed sync root")
    manifest = resolved.manifest
    dataset = validate_slug(str(manifest["dataset"]), "dataset")
    snapshot_id = validate_slug(str(manifest["snapshot_id"]), "snapshot_id")
    dataset_root = materialized_root / dataset
    target = dataset_root / snapshot_id
    ready_path = dataset_root / f".{snapshot_id}.READY.json"
    lock_path = materialized_root / ".locks" / f"fetch-{dataset}-{snapshot_id}.lock"
    growth = (int(manifest["source"]["logical_bytes"])
              + sum(int(manifest["source"][name]) for name in ("files", "directories", "symlinks"))
              * 8192 + 1024**2)
    with _exclusive_lock(lock_path), reserve_cache_write(
            materialized_root, key=resolved.manifest_sha256,
            maximum_growth_bytes=growth, family="materialized",
            enabled=not target.exists()), _verified_packed_inventory(sync_root, resolved,
            d_primary_native_blob_reads=d_primary_native_blob_reads,
            d_primary_native_pack_reads=d_primary_native_pack_reads,
            progress=progress) as (entries, verification):
        if target.exists():
            _verify_materialized(target, manifest, entries)
            if not ready_path.exists():
                atomic_write_json(
                    ready_path,
                    {
                        "snapshot_id": snapshot_id,
                        "manifest_sha256": resolved.manifest_sha256,
                        "verified_at": _utc_iso_from_ns(time.time_ns()),
                    },
                )
            return target
        dataset_root.mkdir(parents=True, exist_ok=True)
        staging = dataset_root / f".{snapshot_id}.partial.{uuid.uuid4().hex}"
        staging.mkdir()
        copied_files = copied_bytes = 0
        def report_copy(row):
            nonlocal copied_files, copied_bytes
            copied_files += 1
            copied_bytes += int(row['size'])
            if progress is not None:
                progress({'stage': 'materialize-cold-archive', 'completed_items': copied_files,
                          'total_items': manifest['source']['files'], 'completed_bytes': copied_bytes,
                          'total_bytes': manifest['source']['logical_bytes']})
        if progress is not None:
            progress({'stage': 'materialize-cold-archive', 'completed_items': 0,
                      'total_items': manifest['source']['files'], 'completed_bytes': 0,
                      'total_bytes': manifest['source']['logical_bytes']})
        try:
            for row in entries:
                if row["kind"] == "directory":
                    path = staging.joinpath(*PurePosixPath(row["path"]).parts)
                    path.mkdir(parents=True, exist_ok=True)
                    path.chmod(int(row["mode"]))
            for row in entries:
                path = staging.joinpath(*PurePosixPath(row["path"]).parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                if row["kind"] == "symlink":
                    path.symlink_to(str(row["target"]))
            _materialize_regular_files(
                sync_root, manifest, entries,
                lambda row: staging.joinpath(*PurePosixPath(row["path"]).parts),
                d_primary_native_blob_reads=d_primary_native_blob_reads,
                d_primary_native_pack_reads=d_primary_native_pack_reads,
                durable=not verification_only, on_copy=report_copy,
            )
            _verify_materialized(staging, manifest, entries)
            if verification_only:
                # This is an ephemeral, fully SHA-verified reconstruction,
                # never a managed target/READY. The caller must still decode
                # and validate all originals before persisting its cold proof.
                return staging
            os.replace(staging, target)
            _fsync_directory(dataset_root)
        except Exception:
            # Keep the partial tree for diagnosis; it is never treated as ready.
            raise
        atomic_write_json(
            ready_path,
            {
                "snapshot_id": snapshot_id,
                "manifest_sha256": resolved.manifest_sha256,
                "inventory_sha256": verification["inventory_sha256"],
                "verified_at": _utc_iso_from_ns(time.time_ns()),
            },
        )
        return target


def _check_packed_read_authority(sync_root: Path) -> None:
    if (sync_root / ".stockagent-d-mount-required").exists():
        raise SnapshotError("D-backed cold volume is not mounted; cannot recreate its authority")
    if sync_root == Path("/srv/stockagent-packed"):
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        _check_d_primary_mount(sync_root)


@contextlib.contextmanager
def _existing_packed_publication_owner(sync_root: Path):
    """Borrow, never recreate, the publisher/retention owner's exact inode."""
    owner = sync_root / ".local-state/locks/publish-retention-global.lock"
    try:
        before = owner.lstat()
        if (owner.resolve(strict=True) != owner or not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1):
            raise SnapshotError("flat restore requires the existing canonical publication owner")
        descriptor = os.open(owner, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise SnapshotError("flat restore cannot open its existing publication owner") from error

    def check_owner():
        try:
            current, held = owner.lstat(), os.fstat(descriptor)
            if (owner.resolve(strict=True) != owner or not stat.S_ISREG(current.st_mode)
                    or held.st_nlink != 1 or current.st_nlink != 1
                    or _source_stat(current) != _source_stat(before)
                    or _source_stat(held) != _source_stat(before)):
                raise SnapshotError("flat restore publication owner changed")
        except OSError as error:
            raise SnapshotError("flat restore publication owner disappeared") from error

    try:
        check_owner()
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        check_owner()  # A replaced pathname is not serialized by this old FD.
        _check_packed_read_authority(sync_root)
        yield
        check_owner()
        _check_packed_read_authority(sync_root)
    finally:
        os.close(descriptor)


def stage_packed_root_files(
    sync_root: Path,
    destination_root: Path,
    resolved: ResolvedSnapshot,
    *,
    expected_names: Iterable[str],
    maximum_bytes: int,
    d_primary_native_blob_reads: bool = False,
    d_primary_native_pack_reads: bool = False,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Restore an entire small root-file release without a second hot tree.

    Files get fresh, exclusive temporary names in the consumer's namespace;
    existing files and unknown names are untouched. The caller must own its
    original key lock and reserve the complete growth BEFORE calling, then
    validate its ABI and commit or retire only these exact temporary files.
    Failed stages are retained and remain visible to namespace byte accounting.
    No managed materialization, READY, cold binding or consumer commit is made.
    """
    names = _normalize_included_root_files(expected_names)
    if names is None or type(maximum_bytes) is not int or not 0 < maximum_bytes < 2**63:
        raise SnapshotError("flat restore requires explicit root names and a positive byte bound")
    sync_root, root = sync_root.absolute(), destination_root.absolute()
    _check_packed_read_authority(sync_root)
    requested_root, sync_root = sync_root, sync_root.resolve()
    if sync_root != requested_root:
        _check_packed_read_authority(sync_root)
    if root.resolve(strict=True) != root or _paths_overlap(sync_root, root):
        raise SnapshotError("flat restore destination must be real and outside cold authority")
    before = root.lstat()
    if (not stat.S_ISDIR(before.st_mode) or before.st_uid != os.geteuid()
            or before.st_mode & 0o022):
        raise SnapshotError("flat restore requires a private-owner destination")

    def check_root():
        after = root.lstat()
        if (root.resolve(strict=True) != root
                or (after.st_dev, after.st_ino, after.st_mode, after.st_uid)
                != (before.st_dev, before.st_ino, before.st_mode, before.st_uid)):
            raise SnapshotError("flat restore destination directory changed")

    manifest = resolved.manifest
    _validate_manifest(manifest)
    source = manifest["source"]
    selection = source.get("selection", {})
    if (not isinstance(selection, Mapping) or selection.get("included_root_files") != list(names)
            or type(source.get("files")) is not int or source["files"] != len(names)
            or type(source.get("logical_bytes")) is not int
            or not 0 <= source["logical_bytes"] <= maximum_bytes
            or sum(int(item["bytes"]) for item in manifest["archive"]["objects"])
            > 2 * maximum_bytes + 1024**2):
        raise SnapshotError("flat restore requires the complete bounded root-file release")
    prefix = f".packed-stage-{uuid.uuid4().hex}-"
    name_max = os.pathconf(root, "PC_NAME_MAX")
    if any(len(os.fsencode(prefix + name)) > name_max for name in names):
        raise SnapshotError("flat restore temporary name exceeds the destination bound")
    paths = {name: root / (prefix + name) for name in names}
    # Retention cannot retire an object between fixed-release verification and
    # its streamed reconstruction. Do not create a new lock or moving head.
    with _existing_packed_publication_owner(sync_root):
        check_root()
        verification = verify_packed_snapshot(
            sync_root, resolved, d_primary_native_blob_reads=d_primary_native_blob_reads,
            d_primary_native_pack_reads=d_primary_native_pack_reads,
            **({"progress": progress} if progress is not None else {}))
        entries = _load_inventory(sync_root, manifest)
        _validate_inventory(manifest, entries)
        if sorted(row["path"] for row in entries) != list(names):
            raise SnapshotError("flat restore inventory differs from requested complete scope")
        check_root()
        _materialize_regular_files(
            sync_root, manifest, entries, lambda row: paths[row["path"]],
            d_primary_native_blob_reads=d_primary_native_blob_reads,
            d_primary_native_pack_reads=d_primary_native_pack_reads, durable=True,
        )
        for row in entries:
            path = paths[row["path"]]
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_size != int(row["size"])
                    or stat.S_IMODE(info.st_mode) != int(row["mode"])
                    or info.st_nlink != 1 or info.st_uid != os.geteuid()
                    or sha256_file(path) != row["sha256"]
                    or _source_stat(path.lstat()) != _source_stat(info)):
                raise SnapshotError(f"flat reconstructed file differs: {path}")
        check_root()
        _fsync_directory(root)
        return {"contract": "packed_explicit_root_file_stage_v1", "files": paths,
                "snapshot_id": manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256,
                "inventory_sha256": verification["inventory_sha256"],
                "logical_bytes": source["logical_bytes"], "managed_materialization_created": False,
                "consumer_commit": False, "failure_policy": "retain_exact_private_stages"}


def resolve_packed_pin(
    sync_root: Path,
    pin_path: Path,
    *,
    expected_dataset: str | None = None,
    maximum_pin_bytes: int = MAX_PACKED_PIN_BYTES,
) -> ResolvedSnapshot:
    """Read the standard pin's exact release, never the current/moving head.

    A pin is a fixed binding, not source/consumer acceptance or permission to
    evict. Validate its bounded bytes and immutable manifest independently;
    subsequent staging still checks every object and reconstructed file.
    """
    if type(maximum_pin_bytes) is not int or not 0 < maximum_pin_bytes <= MAX_PACKED_PIN_BYTES:
        raise SnapshotError("packed pin needs a positive bounded metadata size")
    if expected_dataset is not None:
        if not isinstance(expected_dataset, str):
            raise SnapshotError("expected pin dataset must be explicit")
        validate_slug(expected_dataset, "expected pin dataset")
    pin_path = pin_path.absolute()
    try:
        before = pin_path.lstat()
        if (pin_path.resolve(strict=True) != pin_path or not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1 or not 0 < before.st_size <= maximum_pin_bytes):
            raise SnapshotError("untrusted or oversized packed pin")
        descriptor = os.open(pin_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            raw = os.read(descriptor, maximum_pin_bytes + 1)
            if (_source_stat(os.fstat(descriptor)) != _source_stat(before)
                    or _source_stat(pin_path.lstat()) != _source_stat(before)
                    or len(raw) != before.st_size):
                raise SnapshotError("packed pin changed during read")
        finally:
            os.close(descriptor)

        def unique_fields(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise SnapshotError("duplicate packed pin field")
                value[key] = item
            return value

        pin = json.loads(raw, object_pairs_hook=unique_fields)
        if (not isinstance(pin, dict)
                or set(pin) != {"schema_version", "resolved_at", "manifest_sha256", "manifest"}
                or type(pin["schema_version"]) is not int or pin["schema_version"] != 1
                or not isinstance(pin["resolved_at"], str) or not 0 < len(pin["resolved_at"]) <= 64
                or not isinstance(pin["manifest"], dict)):
            raise SnapshotError("unsupported packed pin schema")
        _validate_manifest(pin["manifest"])
        wanted_sha = _validate_hash(pin["manifest_sha256"], "pin manifest SHA-256")
        dataset = pin["manifest"]["dataset"]
        if expected_dataset is not None and dataset != expected_dataset:
            raise SnapshotError("packed pin belongs to another dataset")
        sync_root = sync_root.absolute()
        _check_packed_read_authority(sync_root)
        requested_root, sync_root = sync_root, sync_root.resolve()
        if sync_root != requested_root:
            _check_packed_read_authority(sync_root)
        fixed = resolve_packed_snapshot_id(sync_root, dataset, pin["manifest"]["snapshot_id"])
        if fixed.manifest_sha256 != wanted_sha or fixed.manifest != pin["manifest"]:
            raise SnapshotError("packed pin differs from its exact immutable manifest")
        if _source_stat(pin_path.lstat()) != _source_stat(before):
            raise SnapshotError("packed pin changed during fixed resolution")
        return fixed
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise SnapshotError(f"cannot resolve exact packed pin: {error}") from error


def write_packed_pin(path: Path, resolved: ResolvedSnapshot, *, exclusive: bool = False) -> None:
    if type(exclusive) is not bool:
        raise SnapshotError("exclusive packed pin creation must be an explicit boolean")
    value = {
        "schema_version": 1,
        "resolved_at": _utc_iso_from_ns(time.time_ns()),
        "manifest_sha256": resolved.manifest_sha256,
        "manifest": resolved.manifest,
    }
    if not exclusive:
        atomic_write_json(path, value)
        return
    # Preserve the existing standard pin format and durable writer. link(2)
    # installs its complete inode without replacing an unexpected existing
    # binding; the cooperating consumer still owns its original key lock.
    temporary = path.with_name(f".{path.name}-{uuid.uuid4().hex}.pending")
    atomic_write_json(temporary, value)
    try:
        os.link(temporary, path, follow_symlinks=False)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink()
        _fsync_directory(path.parent)


def referenced_packed_objects(sync_root: Path) -> set[Path]:
    """Return objects referenced by valid immutable manifests without deleting any."""

    sync_root = sync_root.resolve()
    referenced: set[Path] = set()
    for manifest_path in sorted((sync_root / "manifests").glob("*/*.json")):
        try:
            manifest = _load_json(manifest_path)
            _validate_manifest(manifest)
        except (OSError, SnapshotError):
            continue
        inventory = manifest["archive"]["inventory"]
        referenced.add(
            _path_under(sync_root, str(inventory["relpath"]), "inventory relpath")
        )
        for item in manifest["archive"]["objects"]:
            referenced.add(
                _path_under(sync_root, str(item["relpath"]), "object relpath")
            )
    return referenced


def audit_packed_store(
    sync_root: Path,
    *,
    progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Hash every stored object and validate every immutable manifest.

    This is deliberately read-only. Unreferenced content-addressed objects are
    reported but retained because fleet-wide reachability is a separate proof.
    """

    sync_root = sync_root.resolve()
    expected: dict[Path, tuple[str, int]] = {}
    manifests = sorted((sync_root / "manifests").glob("*/*.json"))
    datasets: set[str] = set()
    for manifest_path in manifests:
        manifest = _load_json(manifest_path)
        _validate_manifest(manifest)
        dataset = str(manifest["dataset"])
        snapshot_id = str(manifest["snapshot_id"])
        if manifest_path.parent.name != dataset or manifest_path.stem != snapshot_id:
            raise SnapshotError(f"manifest path does not match identity: {manifest_path}")
        datasets.add(dataset)
        archive = manifest["archive"]
        for item in [archive["inventory"], *archive["objects"]]:
            path = _path_under(sync_root, str(item["relpath"]), "object relpath")
            descriptor = (_validate_hash(item["sha256"], "object SHA-256"), int(item["bytes"]))
            previous = expected.setdefault(path, descriptor)
            if previous != descriptor:
                raise SnapshotError(f"conflicting object descriptor: {path}")

    for dataset in sorted(datasets):
        resolve_latest_packed(sync_root, dataset)

    stored: list[Path] = []
    for kind in ("inventories", "blobs", "packs"):
        parent = sync_root / "objects" / kind
        if not parent.exists():
            continue
        for path in sorted(item for item in parent.rglob("*") if item.is_file()):
            relative = path.relative_to(parent)
            if len(relative.parts) != 2 or relative.parts[0] != path.name[:2]:
                raise SnapshotError(f"non-canonical packed object path: {path}")
            _validate_hash(path.name.split(".", 1)[0], "stored object SHA-256")
            stored.append(path.resolve())

    stored_set = set(stored)
    missing = sorted(path for path in expected if path not in stored_set)
    if missing:
        raise SnapshotError(f"referenced packed object is missing: {missing[0]}")

    verified_bytes = 0
    for index, path in enumerate(stored, start=1):
        digest = path.name.split(".", 1)[0]
        if sha256_file(path) != digest:
            raise SnapshotError(f"stored packed object checksum mismatch: {path}")
        size = path.stat().st_size
        if path in expected and expected[path] != (digest, size):
            raise SnapshotError(f"stored packed object descriptor mismatch: {path}")
        verified_bytes += size
        if progress is not None and (index % 100 == 0 or index == len(stored)):
            progress(index, verified_bytes)

    referenced_set = set(expected)
    return {
        "schema_version": 1,
        "root": str(sync_root),
        "valid_manifests": len(manifests),
        "resolved_datasets": len(datasets),
        "stored_objects": len(stored_set),
        "referenced_objects": len(referenced_set),
        "unreferenced_objects": len(stored_set - referenced_set),
        "unreferenced_bytes": sum(
            path.stat().st_size for path in stored_set - referenced_set
        ),
        "verified_bytes": verified_bytes,
        "all_stored_object_sha256_valid": True,
        "deleted": 0,
    }
