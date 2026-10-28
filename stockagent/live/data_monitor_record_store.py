"""Transactional storage for the private record cache, retaining its v10 values.

The canonical .json path is either the legacy full JSON or a small versioned
descriptor. A descriptor names one local SQLite database; one DELETE-journal
transaction publishes metadata and changed file entries together. There is no
WAL, delta chain, periodic full rewrite, source-stat exemption or second owner.
Public JSON and the logical record/feature ABI are unchanged.
"""
from __future__ import annotations

from contextlib import closing
from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from types import MappingProxyType
from uuid import uuid4

from stockagent import private_json
from stockagent.live.data_monitor_record_codec import (
    decode_footer_stats, decode_footer_stats_pool, decode_schema_references, encode_footer_stats,
)


STORAGE_VERSION = 1
METADATA_STORAGE_VERSION = 2
FACTORED_STORAGE_VERSION = 3
INTERNED_STORAGE_VERSION = 4
APPLICATION_ID = 1396786756
MIN_INCREMENTAL_FILES = 1_024
_PREFIX = b'{"record_cache_storage":'
_DATABASE_NAME = re.compile(r"record_inventory_store\.[0-9a-f]{32}\.sqlite3\Z")
_SECTIONS = ("schemas", "datasets", "dataset_memberships")


def _cache_location(path: Path) -> tuple[Path, int]:
    """Resolve only a strict local descriptor; legacy files resolve to self."""
    with path.open("rb") as stream:
        prefix = stream.read(len(_PREFIX))
        if prefix != _PREFIX:
            return path, 0
        body = prefix + stream.read(4_096)
    descriptor = private_json.loads(body)
    if (not isinstance(descriptor, dict)
            or set(descriptor) != {"record_cache_storage", "database"}
            or type(descriptor["record_cache_storage"]) is not int
            or descriptor["record_cache_storage"] not in {STORAGE_VERSION, METADATA_STORAGE_VERSION, FACTORED_STORAGE_VERSION, INTERNED_STORAGE_VERSION}
            or not isinstance(descriptor["database"], str)
            or not _DATABASE_NAME.fullmatch(descriptor["database"])):
        raise ValueError("invalid record cache storage descriptor")
    target = path.with_name(descriptor["database"])
    if not stat.S_ISREG(target.lstat().st_mode):
        raise ValueError("record cache database must be a regular file")
    return target, descriptor["record_cache_storage"]


def cache_file(path: Path) -> Path:
    """Resolve the legacy file or a supported, strictly local database."""
    return _cache_location(path)[0]


def _identity(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("record cache must be a regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _check_database(connection, expected_version=None):
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if (connection.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or version not in {STORAGE_VERSION, METADATA_STORAGE_VERSION, FACTORED_STORAGE_VERSION, INTERNED_STORAGE_VERSION}
            or expected_version is not None and version != expected_version
            or connection.execute("PRAGMA journal_mode").fetchone()[0] != "delete"):
        raise ValueError("unsupported record cache database contract")
    return version


def _member_document(connection, query, parameters=()):
    # SQL concatenates the stored JSON key prefix with the unchanged value.
    # No JSON SQL functions interpret numbers or coerce legacy NaN/Infinity.
    def members():
        for (value,) in connection.execute(query, parameters):
            if type(value) is not bytes:
                raise ValueError("invalid record cache member encoding")
            yield value
    return b",".join(members())


def _read_only(*args, **kwargs):
    raise TypeError("cached footer stats are read-only; copy before changing them")


class _FooterVector(list):
    """Private owner-frame vector, preserving existing list value semantics."""

    __slots__ = ("_frozen",)

    def __init__(self, values):
        if hasattr(self, "_frozen"):
            _read_only()
        list.__init__(self, values)
        object.__setattr__(self, "_frozen", True)

    __setitem__ = __delitem__ = __iadd__ = __imul__ = _read_only
    __setattr__ = __delattr__ = _read_only
    append = clear = extend = insert = pop = remove = reverse = sort = _read_only


@dataclass(frozen=True, slots=True)
class _FooterFrame:
    path: str
    identity: tuple
    references: object
    blocks: object


class _FooterStats(dict):
    """Validated codec-1 facts and their original bytes in one read-only frame.

    This is an internal ownership contract, not a security boundary against
    callers explicitly invoking base-class mutation methods. Normal readers
    still receive plain, independently mutable dictionaries and lists.
    """

    __slots__ = ("_encoded_block", "_frame")

    def __init__(self, values, block, frame):
        if hasattr(self, "_encoded_block"):
            _read_only()
        dict.__init__(self, values, non_null=_FooterVector(values["non_null"]))
        object.__setattr__(self, "_encoded_block", block)
        object.__setattr__(self, "_frame", frame)

    __setitem__ = __delitem__ = __ior__ = __setattr__ = __delattr__ = _read_only
    clear = pop = popitem = setdefault = update = _read_only


def _hydrate_stats(connection, payload, *, share_stats=False, immutable_stats=False, references=None, origin=None):
    bodies, codecs, members = {}, {}, []
    for key, body, codec in connection.execute("SELECT key,body,codec FROM footer_stats ORDER BY rowid"):
        if type(key) is not str or type(body) is not bytes or hashlib.sha256(body).hexdigest() != key:
            raise ValueError("invalid footer stats block identity")
        if not share_stats and codec == 0 or immutable_stats:
            bodies[key] = body
        codecs[key] = codec
        members.append(b'"' + key.encode("ascii") + b'":' + body)
    blocks = decode_footer_stats_pool(b"{" + b",".join(members) + b"}", codecs, typed_decode=True)
    del members
    references = (connection.execute("SELECT path,stats_key FROM files NOT INDEXED ORDER BY rowid")
                  if references is None else references)
    if immutable_stats:
        references = list(references)
        frame = _FooterFrame(origin[0], origin[1], MappingProxyType(dict(references)), MappingProxyType(bodies))
        for key, stats in blocks.items():
            if codecs[key] == 1:
                blocks[key] = _FooterStats(stats, (key, bodies[key], 1), frame)
    for (path, entry), (expected_path, key) in zip(payload["files"].items(), references, strict=True):
        if path != expected_path:
            raise ValueError("invalid record cache file reference order")
        if key is None:
            continue
        if type(entry) is not dict or "stats" in entry or key not in blocks:
            raise ValueError("invalid record cache stats reference")
        stats, codec = blocks[key], codecs[key]
        if share_stats:
            entry["stats"] = stats
        elif codec == 1:
            # All other witnessed fields are immutable scalars. Each public
            # result retains independent mutable dictionaries and lists.
            entry["stats"] = {**stats, "non_null": stats["non_null"].copy()}
        else:
            entry["stats"] = decode_footer_stats(bodies[key], codec)
    return payload


class SharedSchema(Sequence):
    """Frame-only validated references; public readers receive plain lists."""

    __slots__ = ("_indices", "_pool")

    def __init__(self, indices, pool):
        self._indices, self._pool = indices, pool

    def __len__(self):
        return len(self._indices)

    def __iter__(self):
        return (self._pool[index] for index in self._indices)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return [self._pool[index] for index in self._indices[key]]
        return self._pool[self._indices[key]]

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, (list, SharedSchema)):
            return False
        return len(self) == len(other) and all(left == right for left, right in zip(self, other, strict=True))


def _read_schemas(connection, *, share_fields, compact_schemas=False):
    """Expand references, sharing immutable-use field pairs only in the frame.

    The intern table retains historical pairs for this database's lifetime;
    changed schemas never require a full field-pool rewrite or an edge census.
    Ordinary readers still receive independently mutable lists at every pair.
    """
    fields = [None]
    for key, name, dtype in connection.execute("SELECT id,name,dtype FROM schema_fields ORDER BY id"):
        if type(key) is not int or key != len(fields) or type(name) is not str or type(dtype) is not str:
            raise ValueError("invalid record cache schema field")
        fields.append([name, dtype])
    references, codecs = {}, {}
    members = []
    for key, name, body, codec in connection.execute(
            "SELECT key,name,body,codec FROM sections NOT INDEXED WHERE section='schemas' ORDER BY rowid"):
        if type(key) is not str or type(name) is not bytes or type(body) is not bytes or type(codec) is not int or codec not in (0, 1):
            raise ValueError("invalid record cache schema block")
        if name != private_json.dumps(key) + b":":
            raise ValueError("invalid record cache schema name")
        references[key] = body
        codecs[key] = codec
        members.append(name + body)
    try:
        values, typed_indices = decode_schema_references(b"{" + b",".join(members) + b"}", codecs)
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid record cache schema field reference") from exc
    if values.keys() != references.keys():
        raise ValueError("invalid record cache schema references")
    for key, value in values.items():
        if codecs[key] == 0:
            continue
        if (type(value) is not list or not typed_indices and any(type(index) is not int for index in value)
                or min(value, default=1) < 1 or max(value, default=0) >= len(fields)):
            raise ValueError("invalid record cache schema field reference")
        values[key] = (SharedSchema(value, fields) if compact_schemas
                       else [fields[index] for index in value] if share_fields
                       else [fields[index].copy() for index in value])
    return values


def read_record_cache(path: Path, *, share_stats=False, compact_schemas=False, immutable_stats=False):
    """Materialize exactly one complete SQLite read transaction or legacy JSON."""
    if (compact_schemas or immutable_stats) and not share_stats:
        raise ValueError("compact schemas and immutable stats require the read-only owner frame")
    target, version = _cache_location(path)
    if target == path:
        return private_json.loads(path.read_bytes())
    origin = (str(target.absolute()), tuple(_identity(target))) if immutable_stats else None
    with closing(sqlite3.connect(target.absolute().as_uri() + "?mode=ro", uri=True, timeout=30.)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        _check_database(connection, version)
        row = connection.execute("SELECT body FROM metadata WHERE key='payload'").fetchone()
        if row is None or type(row[0]) is not bytes:
            raise ValueError("missing record cache metadata")
        payload = private_json.loads(row[0])
        if not isinstance(payload, dict) or "files" in payload:
            raise ValueError("invalid record cache metadata")
        if version >= METADATA_STORAGE_VERSION:
            layout = connection.execute("SELECT body FROM metadata WHERE key='sections'").fetchone()
            names = private_json.loads(layout[0]) if layout and type(layout[0]) is bytes else None
            if (type(names) is not list or any(type(name) is not str or name not in _SECTIONS for name in names)
                    or len(set(names)) != len(names) or any(name in payload for name in names)):
                raise ValueError("invalid record cache metadata sections")
            body = bytearray(row[0].rstrip(b" \t\r\n")[:-1])
            populated = bool(payload)
            references = None
            for name in [*names, "files"]:
                if version >= INTERNED_STORAGE_VERSION and name == "schemas":
                    continue
                if populated:
                    body.extend(b",")
                populated = True
                body.extend(private_json.dumps(name) + b":{")
                if name == "files":
                    if version >= FACTORED_STORAGE_VERSION:
                        references, chunks = [], []
                        for key, member, stats_key in connection.execute(
                                "SELECT path,CAST(name||body AS BLOB),stats_key FROM files ORDER BY rowid"):
                            if type(key) is not str or type(member) is not bytes:
                                raise ValueError("invalid record cache row encoding")
                            references.append((key, stats_key))
                            chunks.append(member)
                        members = b",".join(chunks)
                        del chunks
                    else:
                        members = _member_document(connection,
                            "SELECT CAST(name||body AS BLOB) FROM files ORDER BY rowid")
                else:
                    members = _member_document(connection,
                        # An indexed section lookup followed by rowid sorting
                        # spills the wide JSON values into a temporary B-tree.
                        # This small metadata table is already in rowid order;
                        # scan its headers and retain only the chosen section.
                        "SELECT CAST(name||body AS BLOB) FROM sections NOT INDEXED WHERE section=? ORDER BY rowid", (name,))
                body.extend(members)
                del members
                body.extend(b"}")
            body.extend(b"}")
            payload = private_json.loads(body)
            if version >= INTERNED_STORAGE_VERSION and "schemas" in names:
                payload["schemas"] = _read_schemas(connection, share_fields=share_stats,
                                                    compact_schemas=compact_schemas)
            if version >= FACTORED_STORAGE_VERSION:
                payload = _hydrate_stats(connection, payload, share_stats=share_stats,
                                         immutable_stats=immutable_stats, references=references, origin=origin)
            if origin is not None and tuple(_identity(target)) != origin[1]:
                raise ValueError("record cache changed during immutable frame read")
            return payload
        # Decode one complete document in native code. Calling the decoder
        # once per row creates tens of thousands of growing Python object
        # graphs separately and adds substantial dispatch/GC overhead. The
        # bytearray needs one bounded full-document buffer, like legacy JSON;
        # row bytes remain exactly the originals, including nonfinite values.
        body = bytearray(row[0].rstrip(b" \t\r\n")[:-1])
        if payload:
            body.extend(b",")
        body.extend(b'"files":{')
        first = True
        for key, value in connection.execute("SELECT path,body FROM files ORDER BY rowid"):
            if type(key) is not str or type(value) is not bytes:
                raise ValueError("invalid record cache row encoding")
            if not first:
                body.extend(b",")
            first = False
            body.extend(private_json.dumps(key))
            body.extend(b":")
            body.extend(value)
        body.extend(b"}}")
        return private_json.loads(body)


def stable_record_cache_bytes(path: Path) -> bytes:
    """Freeze full logical JSON for offline tools, fencing head AND database.

    Flat legacy bytes remain byte-identical. Transactional caches are exported
    only for these tools, never re-encoded by the producer on each update.
    """
    head_before = _identity(path)
    target = cache_file(path)
    before = _identity(target)
    body = path.read_bytes() if target == path else private_json.dumps(read_record_cache(path))
    if (cache_file(path) != target or _identity(path) != head_before
            or _identity(target) != before):
        raise ValueError("cache advanced while capturing")
    return body


def _metadata_bytes(payload):
    return private_json.dumps({key: value for key, value in payload.items() if key != "files"})


def _metadata_sections(payload):
    sections = {name: payload[name] for name in _SECTIONS if type(payload.get(name)) is dict}
    if any(type(key) is not str for values in sections.values() for key in values):
        raise ValueError("record cache metadata section keys must be strings")
    root = {key: value for key, value in payload.items() if key != "files" and key not in sections}
    return private_json.dumps(root), private_json.dumps(list(sections)), sections


def record_store_needs_migration(path: Path, files_total: int, *, partitioned_metadata=False, factored_stats=False,
                                 interned_schemas=False) -> bool:
    if files_total < MIN_INCREMENTAL_FILES:
        return False
    try:
        _, version = _cache_location(path)
        return version == 0 or (interned_schemas and version < INTERNED_STORAGE_VERSION
                               or factored_stats and version < FACTORED_STORAGE_VERSION
                               or partitioned_metadata and version < METADATA_STORAGE_VERSION)
    except (OSError, ValueError):
        return True


def _split_file(value, *, stats_pool=None):
    if type(value) is dict and type(value.get("stats")) is _FooterStats:
        # The same frame owns immutable decoded facts and SHA-checked bytes.
        # A header change cannot change this block. New mutable facts continue
        # through the ordinary type proof, encoding and content hash below.
        return (private_json.dumps({name: item for name, item in value.items() if name != "stats"}),
                value["stats"]._encoded_block)
    if type(value) is dict and type(value.get("stats")) is dict:
        stats = value["stats"]
        cached = stats_pool.get(id(stats)) if stats_pool is not None else None
        if cached is None or cached[0] is not stats:
            body, codec = encode_footer_stats(stats)
            block = (hashlib.sha256(body).hexdigest(), body, codec)
            if stats_pool is not None:
                # Retain the object as well as its id. The pool is private to
                # this publication; no mutable proof survives a transaction.
                stats_pool[id(stats)] = (stats, block)
        else:
            block = cached[1]
        return private_json.dumps({name: item for name, item in value.items() if name != "stats"}), block
    return private_json.dumps(value), None


def _ensure_stats(connection, block, seen, *, proven_blocks=None):
    key, body, codec = block
    if key in seen:
        if seen[key] != body:
            raise ValueError("conflicting footer stats identity")
        return 0
    if proven_blocks is not None:
        row = (proven_blocks[key],) if key in proven_blocks else None
    else:
        row = connection.execute("SELECT body FROM footer_stats WHERE key=?", (key,)).fetchone()
    if row is None:
        connection.execute("INSERT INTO footer_stats VALUES(?,?,?)", (key, body, codec))
    elif row[0] != body:
        raise ValueError("conflicting footer stats identity")
    seen[key] = body
    return int(row is None)


class _SchemaWriter:
    """One transaction's exact string-pair intern map, never a source cache."""

    def __init__(self, connection):
        self.connection = connection
        self.fields = {(name, dtype): key for key, name, dtype in
                       connection.execute("SELECT id,name,dtype FROM schema_fields")}

    def encode(self, value):
        if isinstance(value, SharedSchema):
            value = list(value)
        if (type(value) is not list or any(type(pair) is not list or len(pair) != 2
                or type(pair[0]) is not str or type(pair[1]) is not str for pair in value)):
            return private_json.dumps(value), 0
        try:
            for name, dtype in value:
                name.encode("utf-8")
                dtype.encode("utf-8")
        except UnicodeError:
            return private_json.dumps(value), 0
        indices = []
        for name, dtype in value:
            pair = name, dtype
            key = self.fields.get(pair)
            if key is None:
                key = self.connection.execute(
                    "INSERT INTO schema_fields(name,dtype) VALUES(?,?)", pair).lastrowid
                self.fields[pair] = key
            indices.append(key)
        return private_json.dumps(indices), 1


def _section_rows(connection, sections, *, version, old_sections=None):
    writer = None
    for name, values in sections.items():
        old = (old_sections or {}).get(name, {})
        for key, value in values.items():
            if key in old and old[key] is value:
                continue
            if version >= INTERNED_STORAGE_VERSION:
                if name == "schemas":
                    if writer is None:
                        writer = _SchemaWriter(connection)
                    body, codec = writer.encode(value)
                else:
                    body, codec = private_json.dumps(value), 0
                yield name, key, body, private_json.dumps(key) + b":", codec
            else:
                yield name, key, private_json.dumps(value), private_json.dumps(key) + b":"


def _create(path, payload, *, version=STORAGE_VERSION):
    database = path.with_name(f"record_inventory_store.{uuid4().hex}.sqlite3")
    temporary = database.with_suffix(".tmp")
    # Reserve only this writer's unique file with private permissions.
    with temporary.open("xb"):
        pass
    temporary.chmod(0o600)
    stats_written = 0
    try:
        with closing(sqlite3.connect(temporary, timeout=30.)) as connection:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
            connection.execute(f"PRAGMA user_version={version}")
            with connection:
                connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, body BLOB NOT NULL)")
                if version >= METADATA_STORAGE_VERSION:
                    if version >= FACTORED_STORAGE_VERSION:
                        connection.execute("CREATE TABLE footer_stats(key TEXT PRIMARY KEY, body BLOB NOT NULL, codec INTEGER NOT NULL)")
                        connection.execute("CREATE TABLE files(path TEXT PRIMARY KEY, body BLOB NOT NULL, name BLOB NOT NULL, stats_key TEXT)")
                        connection.execute("CREATE INDEX files_stats_key ON files(stats_key)")
                    else:
                        connection.execute("CREATE TABLE files(path TEXT PRIMARY KEY, body BLOB NOT NULL, name BLOB NOT NULL)")
                    extra = ", codec INTEGER NOT NULL" if version >= INTERNED_STORAGE_VERSION else ""
                    connection.execute("CREATE TABLE sections(section TEXT, key TEXT, body BLOB NOT NULL, name BLOB NOT NULL" + extra + ", PRIMARY KEY(section,key))")
                    if version >= INTERNED_STORAGE_VERSION:
                        connection.execute("CREATE TABLE schema_fields(id INTEGER PRIMARY KEY, name TEXT NOT NULL, dtype TEXT NOT NULL, UNIQUE(name,dtype))")
                    root, layout, sections = _metadata_sections(payload)
                    connection.executemany("INSERT INTO metadata VALUES(?,?)", (("payload", root), ("sections", layout)))
                    placeholders = "?,?,?,?,?" if version >= INTERNED_STORAGE_VERSION else "?,?,?,?"
                    connection.executemany("INSERT INTO sections VALUES(" + placeholders + ")",
                                           _section_rows(connection, sections, version=version))
                    if version >= FACTORED_STORAGE_VERSION:
                        seen = {}
                        stats_pool = {}
                        for key, value in payload["files"].items():
                            body, block = _split_file(value, stats_pool=stats_pool)
                            if block is not None:
                                stats_written += _ensure_stats(connection, block, seen)
                            connection.execute("INSERT INTO files VALUES(?,?,?,?)", (
                                key, body, private_json.dumps(key) + b":", block[0] if block is not None else None))
                    else:
                        connection.executemany("INSERT INTO files VALUES(?,?,?)", (
                            (key, private_json.dumps(value), private_json.dumps(key) + b":")
                            for key, value in payload["files"].items()))
                else:
                    connection.execute("CREATE TABLE files(path TEXT PRIMARY KEY, body BLOB NOT NULL)")
                    connection.execute("INSERT INTO metadata VALUES('payload',?)", (_metadata_bytes(payload),))
                    connection.executemany("INSERT INTO files VALUES(?,?)", (
                        (key, private_json.dumps(value)) for key, value in payload["files"].items()))
        os.replace(temporary, database)
        descriptor = {"record_cache_storage": version, "database": database.name}
        head = path.with_name(f"{path.name}.tmp.{uuid4().hex}")
        try:
            head.write_bytes(private_json.dumps(descriptor))
            os.replace(head, path)
        finally:
            head.unlink(missing_ok=True)
        return len(payload["files"]), stats_written
    finally:
        temporary.unlink(missing_ok=True)


def write_record_delta(path: Path, payload, previous, *, expected_signature,
                       enable: bool = False, partitioned_metadata: bool = False,
                       factored_stats: bool = False, interned_schemas: bool = False) -> dict | None:
    """Publish changed rows only, or decline an unselected legacy migration.

    The inventory owner holds its existing writer flock. The old decoded row
    objects are retained by that owner, so identity safely avoids serialization
    for unchanged rows. New objects are encoded even if Python equality says
    they match: equality alone could discard signed zero or bool/int changes.
    The database identity is fenced inside BEGIN IMMEDIATE before applying a
    delta; a replaced/stale generation instead receives a complete rebuild.
    """
    try:
        target, version = _cache_location(path)
    except (OSError, ValueError):
        target, version = path, 0
    existing = target != path
    if not existing and (not enable or len(payload.get("files", {})) < MIN_INCREMENTAL_FILES):
        return None
    if type(payload) is not dict or type(payload.get("files")) is not dict or any(
            type(key) is not str for key in payload["files"]):
        raise ValueError("incremental record cache requires a dictionary with string file keys")
    started = time.perf_counter()
    selected_version = (INTERNED_STORAGE_VERSION if interned_schemas else FACTORED_STORAGE_VERSION
                        if factored_stats else METADATA_STORAGE_VERSION if partitioned_metadata else STORAGE_VERSION)
    selected_version = max(version, selected_version)
    rows_written, metadata_rows_written, metadata_rows_deleted, removed = 0, 0, 0, []
    stats_rows_written = stats_rows_deleted = 0
    rebuilt = (not existing or version != selected_version or not isinstance(previous, dict)
               or not isinstance(previous.get("files"), dict))
    if existing and not rebuilt:
        old_files = previous.get("files", {}) if isinstance(previous, dict) else {}
        removed = list(old_files.keys() - payload["files"].keys())
        stats_pool = {}
        changed = [(key, _split_file(value, stats_pool=stats_pool) if version >= FACTORED_STORAGE_VERSION else private_json.dumps(value))
                   for key, value in payload["files"].items() if key not in old_files or old_files[key] is not value]
        metadata = _metadata_bytes(payload) if version == STORAGE_VERSION else _metadata_sections(payload)
        with closing(sqlite3.connect(target.absolute().as_uri() + "?mode=rw", uri=True, timeout=30.)) as connection:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            try:
                _check_database(connection, version)
                if expected_signature is None or _identity(cache_file(path)) != expected_signature:
                    rebuilt = True
                    connection.rollback()
                else:
                    old_blocks = set()
                    frame = None
                    if version >= FACTORED_STORAGE_VERSION:
                        target_name, target_identity = str(target.absolute()), tuple(expected_signature)
                        for value in old_files.values():
                            stats = value.get("stats") if type(value) is dict else None
                            if (type(stats) is _FooterStats and stats._frame.path == target_name
                                    and stats._frame.identity == target_identity):
                                frame = stats._frame
                                break
                        for key in removed:
                            row = ((frame.references.get(key),) if frame is not None else
                                   connection.execute("SELECT stats_key FROM files WHERE path=?", (key,)).fetchone())
                            if row and row[0] is not None:
                                old_blocks.add(row[0])
                        for key, (_, block) in changed:
                            new_key = block[0] if block is not None else None
                            # Both the path->block edge and block bytes were
                            # read in this exact fenced database generation.
                            row = ((frame.references.get(key),) if frame is not None else
                                   connection.execute("SELECT stats_key FROM files WHERE path=?", (key,)).fetchone())
                            if row and row[0] is not None and row[0] != new_key:
                                old_blocks.add(row[0])
                    connection.executemany("DELETE FROM files WHERE path=?", ((key,) for key in removed))
                    if version >= METADATA_STORAGE_VERSION:
                        if version >= FACTORED_STORAGE_VERSION:
                            seen = {}
                            for key, (body, block) in changed:
                                if block is not None:
                                    stats_rows_written += _ensure_stats(
                                        connection, block, seen, proven_blocks=frame.blocks if frame is not None else None)
                                changes_before = connection.total_changes
                                connection.execute(
                                    "INSERT INTO files(path,body,name,stats_key) VALUES(?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                                    "body=excluded.body,name=excluded.name,stats_key=excluded.stats_key "
                                    "WHERE body IS NOT excluded.body OR name IS NOT excluded.name OR stats_key IS NOT excluded.stats_key",
                                    (key, body, private_json.dumps(key) + b":", block[0] if block is not None else None))
                                rows_written += connection.total_changes - changes_before
                            changes_before = connection.total_changes
                            connection.executemany("DELETE FROM footer_stats WHERE key=? AND NOT EXISTS "
                                "(SELECT 1 FROM files WHERE stats_key=?)", ((key, key) for key in old_blocks))
                            stats_rows_deleted = connection.total_changes - changes_before
                        else:
                            connection.executemany(
                                "INSERT INTO files(path,body,name) VALUES(?,?,?) ON CONFLICT(path) DO UPDATE SET "
                                "body=excluded.body,name=excluded.name WHERE body IS NOT excluded.body OR name IS NOT excluded.name",
                                ((key, body, private_json.dumps(key) + b":") for key, body in changed))
                        root, layout, sections = metadata
                        old_sections = {name: previous[name] for name in _SECTIONS if type(previous.get(name)) is dict}
                        deleted = [(name, key) for name, values in old_sections.items()
                                   for key in values.keys() - sections.get(name, {}).keys()]
                        changes_before = connection.total_changes
                        connection.executemany("DELETE FROM sections WHERE section=? AND key=?", deleted)
                        metadata_rows_deleted = connection.total_changes - changes_before
                        changes_before = connection.total_changes
                        if version >= INTERNED_STORAGE_VERSION:
                            connection.executemany(
                                "INSERT INTO sections(section,key,body,name,codec) VALUES(?,?,?,?,?) "
                                "ON CONFLICT(section,key) DO UPDATE SET body=excluded.body,name=excluded.name,codec=excluded.codec "
                                "WHERE body IS NOT excluded.body OR name IS NOT excluded.name OR codec IS NOT excluded.codec",
                                _section_rows(connection, sections, version=version, old_sections=old_sections))
                        else:
                            connection.executemany(
                                "INSERT INTO sections(section,key,body,name) VALUES(?,?,?,?) "
                                "ON CONFLICT(section,key) DO UPDATE SET body=excluded.body,name=excluded.name "
                                "WHERE body IS NOT excluded.body OR name IS NOT excluded.name",
                                _section_rows(connection, sections, version=version, old_sections=old_sections))
                        connection.executemany(
                            "INSERT INTO metadata(key,body) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body "
                            "WHERE body IS NOT excluded.body", (("payload", root), ("sections", layout)))
                        metadata_rows_written = connection.total_changes - changes_before
                    else:
                        connection.executemany(
                            "INSERT INTO files(path,body) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET body=excluded.body",
                            changed)
                        connection.execute("UPDATE metadata SET body=? WHERE key='payload'", (metadata,))
                    connection.commit()
                    if version < FACTORED_STORAGE_VERSION:
                        rows_written = len(changed)
            except BaseException:
                connection.rollback()
                raise
    if rebuilt:
        rows_written, stats_rows_written = _create(path, payload, version=selected_version)
        if selected_version >= METADATA_STORAGE_VERSION:
            metadata_rows_written = 2 + sum(len(values) for values in _metadata_sections(payload)[2].values())
    return {"backend": "sqlite_interned_v4" if selected_version == INTERNED_STORAGE_VERSION else "sqlite_factored_v3" if selected_version == FACTORED_STORAGE_VERSION else "sqlite_metadata_v2" if selected_version == METADATA_STORAGE_VERSION else "sqlite_incremental_v1",
            "metadata_rows_written": metadata_rows_written, "metadata_rows_deleted": metadata_rows_deleted,
            "stats_rows_written": stats_rows_written, "stats_rows_deleted": stats_rows_deleted,
            "rows_written": rows_written,
            "rows_deleted": len(removed), "files_total": len(payload["files"]),
            "store_rebuilt": rebuilt, "publication_ms": round((time.perf_counter() - started) * 1_000, 3)}
