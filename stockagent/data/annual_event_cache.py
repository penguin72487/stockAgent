"""Verified, date-row-group cache for immutable annual observation coordinates.

This stores a *derived* coordinate stream, never authoritative observations or
model state. A causal caller must bind all rules and preceding source bytes and
store its exact terminal version subjects. The original sweep still applies
release clocks, TTL, report resets and lifecycle. Each read decodes a whole
release date; neither a row-group boundary nor a cache hit changes availability.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
from typing import Callable, Iterator
from uuid import uuid4

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json

CONTRACT = "annual_observation_coordinates_date_cache_v2"
SCHEMA = {"date": pl.Date, "symbol": pl.String, "feature": pl.String,
          "value": pl.Float64, "period": pl.String}
ROW_GROUP_ROWS = 1 << 20
DISK_RESERVE_BYTES = 30 << 30
PERIOD_SCHEMA = {"symbol": pl.String, "state_key": pl.String, "subject": pl.String}


@dataclass(frozen=True)
class PreparedAnnualCoordinates:
    """Caller-owned canonical events plus optional exact terminal version state.

    Version subjects are strings, not dense ranks. A new cohort can insert
    subjects into its sorted rank map without changing any old ordering; the
    caller remaps these subjects through the original current cohort's ranks.
    Causal caches MUST include the full preceding source prefix and definitions
    in prepare_identity; the generic cache never infers that dependency.
    """

    events: pl.DataFrame
    terminal_periods: pl.DataFrame


class InvalidPreparedEvents(ValueError):
    """A derived cache is not admissible; original inputs remain authoritative."""


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _stat(path: Path) -> tuple:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":")).encode()).hexdigest()


def _same_coordinates(actual: pl.DataFrame, expected: pl.DataFrame) -> bool:
    # NULL and NaN are different events; equality alone also hides signed zero
    # and NaN payloads. Compare every non-NULL float's representation as well.
    return (actual.schema == expected.schema and actual.equals(expected)
            and actual["value"].drop_nulls().to_numpy().view(np.uint64).tobytes()
            == expected["value"].drop_nulls().to_numpy().view(np.uint64).tobytes())


@contextmanager
def _writer_lock(entry: Path):
    if entry.is_symlink():
        raise InvalidPreparedEvents("annual cache entry must not escape through a symlink")
    entry.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (entry / "writer.lock").open("a+b") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


class PreparedObservationCache:
    """One atomic, verified generation per exact input and preparation identity.

    Interrupted and rejected generations are retained for diagnosis, not used.
    An invalid head gets at most one rebuild during this call, under the same
    writer lock. No provider, source mutation, CUDA, GC or remote publication.
    """

    def __init__(self, root: Path, *, reserve_bytes: int = DISK_RESERVE_BYTES):
        self.root = Path(root).absolute()
        if reserve_bytes < 0:
            raise ValueError("negative disk reserve")
        self.reserve_bytes = reserve_bytes
        self.hits = self.builds = self.rejections = 0
        self.build_seconds = self.hit_seconds = 0.0

    def stats(self) -> dict:
        return {"hits": self.hits, "builds": self.builds, "rejections": self.rejections,
                "build_seconds": self.build_seconds, "hit_seconds": self.hit_seconds}

    def prepare(self, source: Path, features: list[str], symbols: list[str],
                prepare_coordinates: Callable, *, prepare_identity: str) -> tuple[Path, dict]:
        started = time.perf_counter()
        source = Path(source).resolve(strict=True)
        if (not prepare_identity or len(set(features)) != len(features)
                or len(set(symbols)) != len(symbols)):
            raise ValueError("explicit preparation identity and unique axes required")
        before = _stat(source)
        identity = {"contract": CONTRACT, "source_sha256": _sha(source),
                    "features": list(features), "symbols": list(symbols),
                    "prepare_identity": prepare_identity, "cache_code_sha256": _sha(Path(__file__)),
                    "polars_version": pl.__version__, "arrow_version": pa.__version__}
        if _stat(source) != before:
            raise ValueError("source changed while fingerprinting annual coordinates")
        key = _digest(identity)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        entry = self.root / key
        with _writer_lock(entry):
            head = entry / "current.json"
            if head.exists():
                try:
                    generation, metadata = self._read(entry, identity)
                    if _stat(source) != before or _sha(source) != identity["source_sha256"]:
                        raise ValueError("source changed while reading annual cache")
                    self.hits += 1
                    self.hit_seconds += time.perf_counter() - started
                    return generation, metadata
                except InvalidPreparedEvents as error:
                    self.rejections += 1
                    atomic_write_json(entry / f"rejected-{uuid4().hex}.json", {
                        "contract": CONTRACT, "reason": str(error),
                        "head_sha256": _sha(head) if not head.is_symlink() and head.is_file() else None})
            estimate = source.stat().st_size * 16 + (16 << 20)
            if shutil.disk_usage(self.root).free < self.reserve_bytes + estimate:
                raise ValueError("annual cache admission would consume the disk reserve")
            prepared = prepare_coordinates(source, features, symbols)
            terminal_periods = None
            if isinstance(prepared, PreparedAnnualCoordinates):
                table, terminal_periods = prepared.events, prepared.terminal_periods
                if (dict(terminal_periods.schema) != PERIOD_SCHEMA
                        or any(terminal_periods[name].null_count() for name in PERIOD_SCHEMA)
                        or terminal_periods.select("symbol", "state_key").is_duplicated().any()):
                    raise ValueError("terminal version subjects must have unique typed state keys")
            else:
                table = prepared
            if (dict(table.schema) != SCHEMA or table["date"].null_count()
                    or not table["date"].is_sorted()):
                raise ValueError("canonical preparation must return sorted typed coordinates")
            generation = entry / f"generation-{uuid4().hex}"
            attempt = entry / f".attempt-{uuid4().hex}"
            attempt.mkdir(mode=0o700)
            payload = attempt / "events.parquet"
            groups = []
            offset = row_group = 0
            with pq.ParquetWriter(payload, table.head(0).to_arrow().schema, compression="zstd",
                                  compression_level=3, use_dictionary=["symbol", "feature", "period"],
                                  write_statistics=["date"], write_page_checksum=True) as writer:
                for rows, day in table["date"].rle().struct.unnest().iter_rows():
                    part = table.slice(offset, rows)
                    writer.write_table(part.to_arrow(), row_group_size=ROW_GROUP_ROWS)
                    count = (rows + ROW_GROUP_ROWS - 1) // ROW_GROUP_ROWS
                    groups.append({"date": day.isoformat(), "rows": rows,
                                   "row_groups": list(range(row_group, row_group + count))})
                    offset += rows
                    row_group += count
            with pq.ParquetFile(payload, page_checksum_verification=True, pre_buffer=False) as reader:
                offset = 0
                for group in groups:
                    actual = pl.from_arrow(reader.read_row_groups(group["row_groups"], use_threads=False))
                    if not _same_coordinates(actual, table.slice(offset, group["rows"])):
                        raise ValueError("prepared annual cache changed source coordinate bits")
                    offset += group["rows"]
            if _stat(source) != before or _sha(source) != identity["source_sha256"]:
                raise ValueError("source changed during annual cache preparation")
            if shutil.disk_usage(self.root).free < self.reserve_bytes:
                raise ValueError("annual cache disk reserve changed during preparation")
            metadata = {"contract": CONTRACT, "identity": identity, "generation": generation.name,
                        "payload": "events.parquet", "sha256": _sha(payload),
                        "bytes": payload.stat().st_size, "rows": table.height, "groups": groups}
            if terminal_periods is not None:
                period_path = attempt / "terminal_periods.parquet"
                terminal_periods.write_parquet(period_path, compression="zstd")
                if not pl.read_parquet(period_path).equals(terminal_periods):
                    raise ValueError("terminal version subjects changed during storage")
                with period_path.open("rb") as stream:
                    os.fsync(stream.fileno())
                period_path.chmod(0o400)
                metadata["terminal_periods"] = {"path": period_path.name, "sha256": _sha(period_path),
                    "bytes": period_path.stat().st_size, "rows": terminal_periods.height}
            if shutil.disk_usage(self.root).free < self.reserve_bytes:
                raise ValueError("annual cache disk reserve changed during terminal-state storage")
            # fsync payload and directory before publishing a durable small head.
            with payload.open("rb") as stream:
                os.fsync(stream.fileno())
            payload.chmod(0o400)
            atomic_write_json(attempt / "manifest.json", metadata)
            os.rename(attempt, generation)
            atomic_write_json(head, metadata)
            self.builds += 1
            prepared = self._read(entry, identity)
            self.build_seconds += time.perf_counter() - started
            return prepared

    def _read(self, entry: Path, identity: dict) -> tuple[Path, dict]:
        try:
            head = entry / "current.json"
            if head.is_symlink() or not head.is_file() or head.stat().st_size > 16 << 20:
                raise InvalidPreparedEvents("annual cache head exceeds bounded metadata")
            metadata = json.loads(head.read_bytes())
            if metadata.get("contract") != CONTRACT or metadata.get("identity") != identity:
                raise InvalidPreparedEvents("annual cache identity mismatch")
            name = metadata["generation"]
            if (not isinstance(name, str) or not name.startswith("generation-")
                    or len(name) != len("generation-") + 32
                    or any(c not in "0123456789abcdef" for c in name[len("generation-"):])):
                raise InvalidPreparedEvents("invalid annual cache generation path")
            generation = entry / name
            payload = generation / "events.parquet"
            if (metadata["payload"] != "events.parquet" or generation.is_symlink()
                    or payload.is_symlink() or not payload.resolve().is_relative_to(entry.resolve())
                    or json.loads((generation / "manifest.json").read_bytes()) != metadata
                    or payload.stat().st_size != metadata["bytes"] or _sha(payload) != metadata["sha256"]):
                raise InvalidPreparedEvents("annual cache payload/manifest proof mismatch")
            with pq.ParquetFile(payload, pre_buffer=False) as reader:
                if dict(pl.from_arrow(reader.schema_arrow.empty_table()).schema) != SCHEMA:
                    raise InvalidPreparedEvents("annual cache schema mismatch")
                previous = None
                expected = 0
                rows = 0
                date_column = reader.schema_arrow.get_field_index("date")
                for group in metadata["groups"]:
                    day = date.fromisoformat(group["date"])
                    indices = group["row_groups"]
                    if (previous is not None and day <= previous) or not indices or group["rows"] <= 0:
                        raise InvalidPreparedEvents("annual cache dates/row groups are not ordered")
                    if indices != list(range(expected, expected + len(indices))):
                        raise InvalidPreparedEvents("annual cache skips/repeats row groups")
                    if sum(reader.metadata.row_group(i).num_rows for i in indices) != group["rows"]:
                        raise InvalidPreparedEvents("annual cache date row count mismatch")
                    for i in indices:
                        statistics = reader.metadata.row_group(i).column(date_column).statistics
                        if (statistics is None or not statistics.has_min_max
                                or statistics.min != day or statistics.max != day
                                or statistics.null_count != 0):
                            raise InvalidPreparedEvents("annual cache date boundary proof mismatch")
                    previous = day
                    expected += len(indices)
                    rows += group["rows"]
                if (expected != reader.num_row_groups or rows != reader.metadata.num_rows
                        or rows != metadata["rows"]):
                    raise InvalidPreparedEvents("annual cache incomplete row coverage")
            if "terminal_periods" in metadata:
                self.read_terminal_periods(generation, metadata)
            return generation, metadata
        except InvalidPreparedEvents:
            raise
        except (OSError, ValueError, KeyError, TypeError, IndexError, pa.ArrowException) as error:
            raise InvalidPreparedEvents(f"unreadable annual cache: {error}") from error

    def iter_dates(self, source: Path, features: list[str], symbols: list[str],
                   prepare_coordinates: Callable, *, prepare_identity: str,
                   on_prepared: Callable | None = None) -> Iterator:
        generation, metadata = self.prepare(source, features, symbols, prepare_coordinates,
                                            prepare_identity=prepare_identity)
        if on_prepared is not None:
            on_prepared(generation, metadata)
        with pq.ParquetFile(generation / "events.parquet", page_checksum_verification=True,
                            pre_buffer=False) as reader:
            for group in metadata["groups"]:
                part = pl.from_arrow(reader.read_row_groups(group["row_groups"], use_threads=False))
                day = date.fromisoformat(group["date"])
                if (part.height != group["rows"] or part["date"].null_count()
                        or part["date"].min() != day or part["date"].max() != day):
                    raise InvalidPreparedEvents("cached coordinates crossed a release-date boundary")
                yield day, part

    def read_terminal_periods(self, generation: Path, metadata: dict) -> pl.DataFrame:
        try:
            proof = metadata["terminal_periods"]
            path = generation / "terminal_periods.parquet"
            if (proof["path"] != path.name or path.is_symlink()
                    or not path.resolve().is_relative_to(self.root.resolve())
                    or path.stat().st_size != proof["bytes"] or _sha(path) != proof["sha256"]):
                raise InvalidPreparedEvents("terminal version subject proof mismatch")
            table = pl.read_parquet(path)
            if (dict(table.schema) != PERIOD_SCHEMA or table.height != proof["rows"]
                    or any(table[name].null_count() for name in PERIOD_SCHEMA)
                    or table.select("symbol", "state_key").is_duplicated().any()):
                raise InvalidPreparedEvents("terminal version subject schema/keys mismatch")
            return table
        except InvalidPreparedEvents:
            raise
        except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as error:
            raise InvalidPreparedEvents(f"unreadable terminal version subjects: {error}") from error
