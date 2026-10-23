"""Disposable, bounded-RAM metadata indexes for the canonical packed pipeline.

The index is never a recovery authority: manifests/inventories/CAS retain that
role. Small releases keep the cheap in-memory path. Large ones spill to a
private local temporary directory instead of retaining millions of Python
objects (and swapping them to disk in an uncontrolled access pattern).
"""
from __future__ import annotations

import json
import hashlib
import os
import shutil
import sqlite3
import stat
import tempfile
import uuid
from contextlib import closing
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, Generic, TypeVar

T = TypeVar("T")
DEFAULT_METADATA_MEMORY_ROWS = 32768
METADATA_SCRATCH_ROOT = Path('/var/tmp')


def _scratch_document(path: Path) -> tuple[dict, str]:
    from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
    info = payload_info(path)
    if (path.resolve(strict=True) != path.absolute() or info['uid'] != os.geteuid()
            or info['size'] > 16 * 1024**2 or info['mode'] & 0o022):
        raise ValueError('scratch provenance is not bounded owned metadata')
    digest = stable_payload_hash(path, info)
    value = json.loads(path.read_bytes())
    if payload_info(path) != info or not isinstance(value, dict):
        raise ValueError('scratch provenance changed during inspection')
    return value, digest


def plan_failed_metadata_scratch(scratch: Path, failed_attempt: Path) -> dict:
    """Explicit failed-operation indexes only; never discover/delete by prefix.

    These SQLite rows are disposable census/reuse metadata, not original
    observations, manifests or CAS. A retained canonical failed attempt must
    name this exact directory, and its activity must have been replaced. The
    existing producer never reopens an old TemporaryDirectory. Unknown roots,
    active/changed/shared files and other SQLite schemas remain protected.
    """
    from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
    from stockagent.data_sync.materialized_cache import process_references_many
    import re

    scratch, failed_attempt = scratch.absolute(), failed_attempt.absolute()
    if (scratch.parent != METADATA_SCRATCH_ROOT or scratch.resolve(strict=True) != scratch
            or re.fullmatch(r'stockagent-packed-metadata-[a-z0-9_]{8}', scratch.name) is None):
        raise ValueError('not an explicit canonical metadata scratch directory')
    before = scratch.lstat()
    if (not stat.S_ISDIR(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o700
            or before.st_uid != os.geteuid()):
        raise ValueError('metadata scratch is not an owned private directory')
    attempt, attempt_sha = _scratch_document(failed_attempt)
    operation = failed_attempt.parent.parent.parent
    plan, plan_sha = _scratch_document(operation / 'plan.json')
    progress, _ = _scratch_document(operation / 'progress.json')
    dataset = failed_attempt.parent.name
    if (failed_attempt.parent.parent.name != 'attempts' or failed_attempt.suffix != '.json'
            or attempt.get('attempt_id') != failed_attempt.stem
            or plan.get('contract') != 'all_catalog_incremental_migration_v1'
            or progress.get('plan_sha256') != plan.get('plan_sha256')
            or dataset not in {row['dataset'] for row in plan['catalog']}
            or attempt.get('state') != 'waiting_retry' or attempt.get('error_type') != 'OSError'
            or attempt.get('error') != f"[Errno 24] Too many open files: '{scratch}'"
            or not attempt.get('finished_at_utc') or not attempt.get('owner_activity_id')
            or not progress.get('runtime_owner', {}).get('activity_id')
            or progress['runtime_owner']['activity_id'] == attempt['owner_activity_id']):
        raise ValueError('scratch has no exact superseded canonical failure provenance')
    if {p.name for p in scratch.iterdir()} != {'index.sqlite'}:
        raise ValueError('retain unknown metadata scratch members')
    database = scratch / 'index.sqlite'
    info = payload_info(database)
    if info['uid'] != os.geteuid() or info['nlink'] != 1 or info['mode'] != 0o600 or info['xattrs']:
        raise ValueError('metadata scratch index is shared or not private')
    keys = {(info['device'], info['inode'])}
    if process_references_many([scratch], inode_keys=keys, include_current_process=True):
        raise ValueError('metadata scratch still has a process reference')
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        schema = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name").fetchall()
        expected_table = ('CREATE TABLE entries (path BLOB PRIMARY KEY, kind TEXT NOT NULL, '
                          'size INTEGER NOT NULL, object_sha TEXT, bucket INTEGER, data TEXT NOT NULL) WITHOUT ROWID')
        expected = {('table', 'entries', 'entries', expected_table),
                    ('index', 'changed_entries', 'entries',
                     'CREATE INDEX changed_entries ON entries(bucket,path) WHERE bucket IS NOT NULL'),
                    ('index', 'object_entries', 'entries',
                     'CREATE INDEX object_entries ON entries(object_sha,path)')}
        if (not schema or any(tuple(row) not in expected for row in schema)
                or ('table', 'entries', 'entries', expected_table) not in schema
                or db.execute('PRAGMA quick_check(1)').fetchall() != [('ok',)]
                or db.execute("SELECT 1 FROM entries WHERE kind NOT IN ('file','directory','symlink') LIMIT 1").fetchone()):
            raise ValueError('scratch is not an intact producer metadata index')
    digest = stable_payload_hash(database, info)
    after = scratch.lstat()
    if (before.st_dev, before.st_ino, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_mtime_ns):
        raise ValueError('metadata scratch directory changed during inspection')
    result = {'contract': 'failed_metadata_index_retirement_v1', 'state': 'eligible',
              'scratch': str(scratch), 'failed_attempt': str(failed_attempt),
              'failed_attempt_sha256': attempt_sha, 'operation_plan_sha256': plan_sha,
              'directory_identity': [before.st_dev, before.st_ino],
              'index': {**info, 'sha256': digest}, 'source_data_files_deleted': 0,
              'cold_objects_deleted': 0}
    result['plan_sha256'] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    return result


def retire_failed_metadata_scratch(plan: dict, receipt: Path) -> dict:
    """Revalidate the exact plan, quarantine, recheck, then unlink one index."""
    from downloader.artifact_io import atomic_write_json
    from stockagent.data_sync.desync_snapshots import _fsync_directory
    from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
    from stockagent.data_sync.materialized_cache import process_references_many

    if receipt.exists() or receipt.is_symlink():
        raise ValueError('retain the original metadata retirement receipt')
    scratch = Path(plan['scratch'])
    if receipt.absolute().is_relative_to(scratch):
        raise ValueError('retirement evidence must remain outside its disposable index')
    if plan_failed_metadata_scratch(scratch, Path(plan['failed_attempt'])) != plan:
        raise ValueError('metadata scratch retirement plan changed')
    quarantine = scratch.with_name('.retired-packed-metadata-' + uuid.uuid4().hex)
    result = {**plan, 'state': 'quarantine_planned', 'quarantine': str(quarantine),
              'allocated_bytes_reclaimed': 0, 'recoverability': 'rebuild_disposable_index_from_original_sources'}
    atomic_write_json(receipt, result, durable=True)
    scratch.rename(quarantine)
    _fsync_directory(scratch.parent)
    database = quarantine / 'index.sqlite'
    expected = plan['index']
    current = payload_info(database)
    # Rename can change directory timestamps, but not the index's own bytes.
    if ({p.name for p in quarantine.iterdir()} != {'index.sqlite'}
            or [quarantine.stat().st_dev, quarantine.stat().st_ino] != plan['directory_identity']
            or current != {k: v for k, v in expected.items() if k != 'sha256'}
            or stable_payload_hash(database, current) != expected['sha256']
            or process_references_many([scratch, quarantine], inode_keys={(current['device'], current['inode'])},
                                       include_current_process=True)):
        raise ValueError('quarantined metadata changed or gained a reference; retained')
    if payload_info(database) != current:
        raise ValueError('metadata index changed before unlink; retained')
    database.unlink()
    _fsync_directory(quarantine)
    quarantine.rmdir()
    _fsync_directory(scratch.parent)
    result.update(state='retired', allocated_bytes_reclaimed=current['allocated'])
    atomic_write_json(receipt, result, durable=True)
    return result


class MetadataRows(Generic[T]):
    def __init__(self, encode: Callable[[T], dict], decode: Callable[[dict], T], *,
                 memory_rows: int = DEFAULT_METADATA_MEMORY_ROWS):
        if type(memory_rows) is not int or memory_rows < 1:
            raise ValueError("metadata memory bound must be positive")
        self._encode, self._decode = encode, decode
        self._limit = memory_rows
        self._rows: list[T] = []
        self._buckets: dict[str, int] = {}
        self._hints: dict[str, T] | None = None
        self._positions: dict[str, int] | None = None
        self._groups: dict[int, list[T]] | None = None
        self._objects: dict[str, list[T]] | None = None
        self._db: sqlite3.Connection | None = None
        self._scratch: tempfile.TemporaryDirectory | None = None
        self._count = 0
        self._work_index = self._object_index = False
        self._cursors: set[sqlite3.Cursor] = set()

    @property
    def spilled(self) -> bool:
        return self._db is not None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self._rows.clear()
        self._positions = None
        self._buckets.clear()
        self._groups = self._objects = None
        if self._hints is not None:
            self._hints.clear()
        if self._db is not None:
            for cursor in self._cursors:
                cursor.close()
            self._cursors.clear()
            self._db.close()
            self._db = None
        if self._scratch is not None:
            self._scratch.cleanup()
            self._scratch = None

    @staticmethod
    def _key(path: str) -> bytes:
        # UTF-8 preserves Python's codepoint ordering, including a surrogate
        # escape in a Linux filename. SQLite TEXT cannot represent surrogates.
        return path.encode("utf-8", errors="surrogatepass")

    def _record(self, value: T, bucket: int | None = None):
        row = self._encode(value)
        storage = row.get("storage") or {}
        return (self._key(row["path"]), row["kind"], int(row.get("size", 0)),
                storage.get("object_sha256"), bucket,
                json.dumps(row, ensure_ascii=True, separators=(",", ":")))

    def _spill(self):
        # mkdtemp creates mode 0700. Only reconstructible metadata lives here;
        # no source file or authoritative cold object is copied or removed.
        # /tmp is tmpfs on penguin: spilling there merely moves the RAM
        # pressure into shmem/swap. /var/tmp is the observed local ext4 SSD.
        self._check_space()
        self._scratch = tempfile.TemporaryDirectory(prefix="stockagent-packed-metadata-", dir="/var/tmp")
        self._db = sqlite3.connect(str(Path(self._scratch.name) / "index.sqlite"))
        self._db.execute("PRAGMA journal_mode=OFF")
        self._db.execute("PRAGMA synchronous=OFF")
        self._db.execute("PRAGMA temp_store=FILE")
        self._db.execute("PRAGMA cache_size=-8192")
        self._db.execute("CREATE TABLE entries (path BLOB PRIMARY KEY, kind TEXT NOT NULL, "
                         "size INTEGER NOT NULL, object_sha TEXT, bucket INTEGER, data TEXT NOT NULL) "
                         "WITHOUT ROWID")
        self._db.executemany("INSERT INTO entries VALUES (?,?,?,?,?,?)",
                             (self._record(row) for row in self._rows))
        self._rows.clear()
        self._positions = None

    @staticmethod
    def _check_space():
        if shutil.disk_usage("/var/tmp").free < 256 * 1024**2:
            raise OSError("packed metadata scratch is below its 256 MiB reserve")

    def append(self, value: T):
        if self._db is None and len(self._rows) == self._limit:
            self._spill()
        if self._db is None:
            if self._positions is not None:
                self._positions[self._encode(value)["path"]] = len(self._rows)
            self._rows.append(value)
        else:
            if self._count % 8192 == 0:
                self._check_space()
            self._db.execute("INSERT INTO entries VALUES (?,?,?,?,?,?)", self._record(value))
        self._count += 1
        self._groups = self._objects = None

    def upsert(self, value: T):
        """Last donor wins by logical path, without a whole-inventory dict.

        Use before consuming file hints. Only the bounded in-memory branch
        needs a second key index; spilled rows use their existing primary key.
        """
        if self._hints is not None:
            raise ValueError("metadata hints have already been consumed")
        if self._db is None:
            if self._positions is None:
                self._positions = {self._encode(row)["path"]: index
                                   for index, row in enumerate(self._rows)}
            position = self._positions.get(self._encode(value)["path"])
            if position is None:
                self.append(value)
            else:
                self._rows[position] = value
        else:
            if self._count % 8192 == 0:
                self._check_space()
            record = self._record(value)
            inserted = self._db.execute("INSERT OR IGNORE INTO entries VALUES (?,?,?,?,?,?)", record)
            if inserted.rowcount:
                self._count += 1
            else:
                path, kind, size, digest, _bucket, payload = record
                self._db.execute("UPDATE entries SET kind=?,size=?,object_sha=?,data=? WHERE path=?",
                                 (kind, size, digest, payload, path))
        self._groups = self._objects = None

    def extend(self, values: Iterable[T]):
        for value in values:
            self.append(value)

    def sort(self, *, key):
        if self._db is None:
            self._rows.sort(key=key)
            self._positions = None

    def __len__(self):
        return self._count

    def _query(self, clause: str = "", parameters=()) -> Iterator[T]:
        cursor = self._db.execute("SELECT data FROM entries " + clause + " ORDER BY path", parameters)
        self._cursors.add(cursor)
        try:
            for (payload,) in cursor:
                yield self._decode(json.loads(payload))
        finally:
            if cursor in self._cursors:
                self._cursors.remove(cursor)
                cursor.close()

    def __iter__(self) -> Iterator[T]:
        if self._db is None:
            yield from self._rows
        else:
            yield from self._query()

    def in_census_order(self) -> Iterator[T]:
        """Original portable scan order without retaining a second inventory.

        Used only by explicitly observed private controls whose sealed sizes
        can differ from the earlier census. SQLite's bounded FILE sorter keeps
        this pass independent of source mutations and Python row count.
        """
        if self._db is None:
            yield from sorted(self._rows, key=lambda value: self._encode(value)['census_order'])
        else:
            self._db.commit()
            cursor = self._db.execute("SELECT data FROM entries ORDER BY CAST(json_extract(data,'$.census_order') AS INTEGER)")
            self._cursors.add(cursor)
            try:
                for row, in cursor:
                    yield self._decode(json.loads(row))
            finally:
                if cursor in self._cursors:
                    self._cursors.remove(cursor)
                    cursor.close()

    def update(self, value: T, *, changed_bucket: int | None = None):
        if self._db is None:
            # In-memory entries retain object identity and are mutated by the
            # caller. Only the bounded changed-file routing needs an index.
            if changed_bucket is not None:
                self._buckets[self._encode(value)["path"]] = changed_bucket
                self._groups = None
            self._objects = None
        else:
            path, kind, size, digest, bucket, payload = self._record(value, changed_bucket)
            if changed_bucket is None:
                # Merely naming bucket in SET makes SQLite maintain the large
                # changed_entries index, even when COALESCE retains its value.
                # Packing changes storage, not the already selected grouping.
                result = self._db.execute("UPDATE entries SET kind=?,size=?,object_sha=?,data=? WHERE path=?",
                                          (kind, size, digest, payload, path))
            else:
                result = self._db.execute("UPDATE entries SET kind=?,size=?,object_sha=?,bucket=?,data=? WHERE path=?",
                                          (kind, size, digest, bucket, payload, path))
            if result.rowcount != 1:
                raise ValueError("metadata update does not identify one census member")

    def update_many(self, values: Iterable[T]):
        """Update an already-routed group without touching its bucket index.

        Consume records one at a time in SQLite's existing transaction; no
        second inventory, commit or durability authority is introduced. A
        missing path still rejects the publication, as individual updates do.
        """
        if self._db is None:
            for value in values:
                self.update(value)
            return
        count = 0
        def records():
            nonlocal count
            for value in values:
                path, kind, size, digest, _, payload = self._record(value)
                count += 1
                yield kind, size, digest, payload, path
        result = self._db.executemany("UPDATE entries SET kind=?,size=?,object_sha=?,data=? WHERE path=?",
                                      records())
        if result.rowcount != count:
            raise ValueError("metadata updates do not identify every census member")

    def changed_groups(self) -> list[tuple[int, int, int]]:
        if self._db is not None:
            if not self._work_index:
                self._db.execute("CREATE INDEX changed_entries ON entries(bucket,path) "
                                 "WHERE bucket IS NOT NULL")
                self._work_index = True
            return list(self._db.execute("SELECT bucket,COUNT(*),SUM(size) FROM entries "
                                         "WHERE bucket IS NOT NULL GROUP BY bucket ORDER BY bucket"))
        if self._groups is None:
            self._groups = {}
            for value in self._rows:
                bucket = self._buckets.get(self._encode(value)["path"])
                if bucket is not None:
                    self._groups.setdefault(bucket, []).append(value)
        return [(bucket, len(group), sum(int(self._encode(row).get("size", 0)) for row in group))
                for bucket, group in sorted(self._groups.items())]

    def changed(self, bucket: int) -> Iterator[T]:
        if self._db is not None:
            yield from self._query("WHERE bucket=?", (bucket,))
        elif self._groups is not None:
            yield from self._groups.get(bucket, ())
        else:
            for value in self._rows:
                if self._buckets.get(self._encode(value)["path"]) == bucket:
                    yield value

    def take_file_hint(self, path: str) -> T | None:
        if self._db is not None:
            # Source paths are unique. The disk index is an immutable hint,
            # not a second RAM inventory: no N DELETEs/page rewrites required.
            found = self._db.execute("SELECT data FROM entries WHERE path=? AND kind='file'",
                                     (self._key(path),)).fetchone()
            return self._decode(json.loads(found[0])) if found else None
        if self._hints is None:
            self._hints = {self._encode(row)["path"]: row for row in self._rows
                           if self._encode(row)["kind"] == "file"}
            self._rows.clear()
            self._positions = None
        return self._hints.pop(path, None)

    def object_rows(self, digest: str) -> Iterator[T]:
        if self._db is not None:
            if not self._object_index:
                self._db.execute("CREATE INDEX object_entries ON entries(object_sha,path)")
                self._object_index = True
            yield from self._query("WHERE object_sha=?", (digest,))
        else:
            if self._objects is None:
                self._objects = {}
                for value in self._rows:
                    object_sha = (self._encode(value).get("storage") or {}).get("object_sha256")
                    if object_sha is not None:
                        self._objects.setdefault(object_sha, []).append(value)
            yield from self._objects.get(digest, ())

    def object_paths(self, digest: str) -> Iterator[str]:
        """Read names from the covering index, not full JSON payload rows.

        ZIP membership checks need only ordered names. Decoding every complete
        inventory row here, then again for full original recovery, needlessly
        doubles row reads/JSON work on multi-million-file releases. The same
        disposable index and exact UTF-8/surrogate ordering remain authoritative
        only for this pass; original SHA/CRC recovery is unchanged.
        """
        if self._db is None:
            for value in self.object_rows(digest):
                yield self._encode(value)['path']
            return
        if not self._object_index:
            self._db.execute("CREATE INDEX object_entries ON entries(object_sha,path)")
            self._object_index = True
        cursor = self._db.execute("SELECT path FROM entries WHERE object_sha=? ORDER BY path", (digest,))
        self._cursors.add(cursor)
        try:
            for (path,) in cursor:
                yield path.decode('utf-8', errors='surrogatepass')
        finally:
            if cursor in self._cursors:
                self._cursors.remove(cursor)
                cursor.close()

    def chunked_rows(self) -> Iterator[T]:
        rows = self._query("WHERE kind='file' AND object_sha IS NULL") if self._db is not None else iter(self)
        for value in rows:
            if (self._encode(value).get("storage") or {}).get("kind") == "chunks":
                yield value

