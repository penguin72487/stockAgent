#!/usr/bin/env python3
"""Build an append-only, queryable L1 layer from completed OpenBB shards.

L0 task shards remain the durable downloader/audit truth.  This command claims
only successful immutable shards that are not already represented by a valid
L1 segment, compacts bounded batches, validates them through PyArrow, Polars,
and DuckDB, then records membership in the same WAL manifest.  It never deletes
L0 data and never exposes an output before row parity succeeds.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Iterator, Mapping, Sequence

import duckdb
import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm

from stockagent.data.columnar_lake import (
    CompactParquetReceipt,
    SourceFileContract,
    compact_parquet_files,
    parquet_schema_fingerprint,
    source_signature,
)


SCHEMA_VERSION = 1
SEGMENT_TABLE = "l1_compaction_segments"
MEMBER_TABLE = "l1_compaction_members"


@dataclass(frozen=True, slots=True)
class TaskShard:
    task_id: str
    endpoint: str
    output_path: str
    rows: int
    task_updated_at: str
    bytes: int
    uncompressed_bytes: int
    mtime_ns: int

    def source_contract(self) -> SourceFileContract:
        return SourceFileContract(
            source_id=f"{self.task_id}\0{self.task_updated_at}",
            path=self.output_path,
            rows=self.rows,
            bytes=self.bytes,
            mtime_ns=self.mtime_ns,
        )


@dataclass(frozen=True, slots=True)
class SegmentAudit:
    segment_id: str
    endpoint: str
    status: str
    source_files: int
    source_rows: int
    output_rows: int
    output_path: str
    arrow_rows: int | None
    polars_rows: int | None
    duckdb_rows: int | None
    error: str | None
    checked_at_utc: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Incrementally compact successful OpenBB task shards into immutable "
            "L1 Parquet segments without deleting source shards."
        )
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_openBB"))
    parser.add_argument(
        "--endpoint",
        action="append",
        default=[],
        help="Exact endpoint or endpoint prefix; repeatable.",
    )
    parser.add_argument("--max-source-files", type=int, default=20_000)
    parser.add_argument("--max-files-per-segment", type=int, default=2_000)
    parser.add_argument(
        "--max-source-bytes-per-segment", type=int, default=256 * 1024 * 1024
    )
    parser.add_argument(
        "--max-uncompressed-bytes-per-segment",
        type=int,
        default=512 * 1024 * 1024,
    )
    parser.add_argument("--max-source-rows-per-segment", type=int, default=10_000_000)
    parser.add_argument("--min-files-per-segment", type=int, default=128)
    parser.add_argument(
        "--include-tail",
        action="store_true",
        help="Compact the final batch even when it is smaller than the minimum.",
    )
    parser.add_argument(
        "--threads", type=int, default=max(1, min(4, os.cpu_count() or 1))
    )
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--row-group-size", type=int, default=122_880)
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Deep-audit active L1 segments, source contracts, and query views.",
    )
    parser.add_argument("--no-progress", action="store_true")
    args = parser.parse_args(argv)
    for name in (
        "max_source_files",
        "max_files_per_segment",
        "max_source_bytes_per_segment",
        "max_uncompressed_bytes_per_segment",
        "max_source_rows_per_segment",
        "min_files_per_segment",
        "threads",
        "row_group_size",
    ):
        if int(getattr(args, name)) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.min_files_per_segment > args.max_files_per_segment:
        parser.error("--min-files-per-segment cannot exceed --max-files-per-segment")
    return args


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _view_name(endpoint: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", endpoint).strip("_").lower()
    if not normalized:
        raise ValueError(f"endpoint does not produce a valid view name: {endpoint!r}")
    return f"openbb_l1_{normalized}"


def _endpoint_matches(endpoint: str, filters: Sequence[str]) -> bool:
    cleaned = [item.strip().lstrip(".") for item in filters if item.strip()]
    return not cleaned or any(
        endpoint == item or endpoint.startswith(f"{item}.") for item in cleaned
    )


def _filter_sql(filters: Sequence[str], *, alias: str = "") -> tuple[str, list[str]]:
    cleaned = [item.strip().lstrip(".") for item in filters if item.strip()]
    if not cleaned:
        return "", []
    column = f"{alias}.endpoint" if alias else "endpoint"
    predicates: list[str] = []
    parameters: list[str] = []
    for item in cleaned:
        predicates.append(f"({column}=? OR {column} LIKE ? ESCAPE '\\')")
        escaped = item.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        parameters.extend((item, f"{escaped}.%"))
    return " AND (" + " OR ".join(predicates) + ")", parameters


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another OpenBB L1 compactor owns {path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} acquired_at_utc={_now()}\n")
        handle.flush()
        os.fsync(handle.fileno())
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _open_manifest(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"OpenBB manifest does not exist: {path}")
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    connection.create_function(
        "stockagent_resolve_path",
        1,
        lambda value: str(Path(str(value)).resolve()),
        deterministic=True,
    )
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=60000")
    connection.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {SEGMENT_TABLE} (
            segment_id TEXT PRIMARY KEY,
            endpoint TEXT NOT NULL,
            output_path TEXT NOT NULL UNIQUE,
            source_signature TEXT NOT NULL,
            source_files INTEGER NOT NULL,
            source_rows INTEGER NOT NULL,
            source_bytes INTEGER NOT NULL,
            source_uncompressed_bytes INTEGER NOT NULL,
            output_rows INTEGER NOT NULL,
            output_bytes INTEGER NOT NULL,
            row_groups INTEGER NOT NULL,
            schema_fingerprint TEXT NOT NULL,
            date_column TEXT,
            min_date TEXT,
            max_date TEXT,
            compression TEXT NOT NULL,
            status TEXT NOT NULL,
            stale_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS {MEMBER_TABLE} (
            task_id TEXT PRIMARY KEY,
            endpoint TEXT NOT NULL,
            segment_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_rows INTEGER NOT NULL,
            source_bytes INTEGER NOT NULL,
            source_uncompressed_bytes INTEGER NOT NULL,
            source_mtime_ns INTEGER NOT NULL,
            task_updated_at TEXT NOT NULL,
            FOREIGN KEY(segment_id) REFERENCES {SEGMENT_TABLE}(segment_id)
        );
        CREATE INDEX IF NOT EXISTS idx_l1_segments_endpoint_status
            ON {SEGMENT_TABLE}(endpoint, status);
        CREATE INDEX IF NOT EXISTS idx_l1_members_segment
            ON {MEMBER_TABLE}(segment_id);
        CREATE INDEX IF NOT EXISTS idx_l1_members_endpoint
            ON {MEMBER_TABLE}(endpoint);
        """
    )
    for table in (SEGMENT_TABLE, MEMBER_TABLE):
        columns = {
            str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
        }
        if "source_uncompressed_bytes" not in columns:
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN "
                "source_uncompressed_bytes INTEGER NOT NULL DEFAULT 0"
            )
    connection.commit()
    return connection


def _active_plan_token(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        "SELECT value FROM archive_meta WHERE key='active_plan_token'"
    ).fetchone()
    return str(row[0]) if row is not None else None


def _quarantine_path(output_dir: Path, segment_id: str, path: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return output_dir / "compact_l1" / "_stale" / segment_id / f"{stamp}-{path.name}"


def _mark_stale_segments(
    connection: sqlite3.Connection,
    filters: Sequence[str],
) -> tuple[set[str], int]:
    filter_clause, filter_parameters = _filter_sql(filters, alias="s")
    stale_endpoints = {
        str(row["endpoint"])
        for row in connection.execute(
            f"""
            SELECT DISTINCT endpoint FROM {SEGMENT_TABLE} AS s
            WHERE status='stale'{filter_clause}
            """,
            filter_parameters,
        )
    }
    rows = connection.execute(
        f"""
        SELECT DISTINCT s.segment_id, s.endpoint, s.output_path,
            CASE
                WHEN t.task_id IS NULL THEN 'source task missing from manifest'
                WHEN t.active!=1 THEN 'source task retired from active plan'
                WHEN t.status!='success' THEN 'source task is no longer successful'
                WHEN t.endpoint!=m.endpoint OR t.endpoint!=s.endpoint
                    THEN 'source endpoint changed'
                WHEN stockagent_resolve_path(t.output_path)!=m.source_path
                    THEN 'source path changed'
                WHEN t.rows!=m.source_rows THEN 'source row contract changed'
                WHEN t.updated_at!=m.task_updated_at THEN 'source task was refreshed'
                ELSE NULL
            END AS stale_reason
        FROM {SEGMENT_TABLE} AS s
        JOIN {MEMBER_TABLE} AS m ON m.segment_id=s.segment_id
        LEFT JOIN tasks AS t ON t.task_id=m.task_id
        WHERE s.status='success'{filter_clause}
          AND (
              t.task_id IS NULL OR t.active!=1 OR t.status!='success'
              OR t.endpoint!=m.endpoint OR t.endpoint!=s.endpoint
              OR stockagent_resolve_path(t.output_path)!=m.source_path
              OR t.rows!=m.source_rows
              OR t.updated_at!=m.task_updated_at
          )
        ORDER BY s.segment_id
        """,
        filter_parameters,
    ).fetchall()
    # Missing derivatives are stale even when the source task contract did not
    # change. Checking one path per segment is bounded by the segment count.
    known = {str(row["segment_id"]): row for row in rows}
    for row in connection.execute(
        f"""
        SELECT segment_id, endpoint, output_path, output_rows, schema_fingerprint
        FROM {SEGMENT_TABLE} AS s
        WHERE status='success'{filter_clause}
        """,
        filter_parameters,
    ):
        if str(row["segment_id"]) in known:
            continue
        path = Path(str(row["output_path"]))
        derivative_error: str | None = None
        try:
            if not path.is_file():
                derivative_error = "L1 output file is missing"
            elif int(pq.ParquetFile(path).metadata.num_rows) != int(
                row["output_rows"]
            ):
                derivative_error = "L1 output row count changed"
            elif parquet_schema_fingerprint(path) != row["schema_fingerprint"]:
                derivative_error = "L1 output schema fingerprint changed"
        except Exception as exc:
            derivative_error = f"L1 output is unreadable: {type(exc).__name__}: {exc}"
        if derivative_error is not None:
            known[str(row["segment_id"])] = {
                "segment_id": row["segment_id"],
                "endpoint": row["endpoint"],
                "output_path": row["output_path"],
                "stale_reason": derivative_error,
            }

    for row in known.values():
        segment_id = str(row["segment_id"])
        stale_endpoints.add(str(row["endpoint"]))
        with connection:
            connection.execute(
                f"""
                UPDATE {SEGMENT_TABLE}
                SET status='stale', stale_reason=?, updated_at=?
                WHERE segment_id=? AND status='success'
                """,
                (str(row["stale_reason"]), _now(), segment_id),
            )
            connection.execute(
                f"DELETE FROM {MEMBER_TABLE} WHERE segment_id=?", (segment_id,)
            )
    return stale_endpoints, len(known)


def _quarantine_stale_outputs(
    connection: sqlite3.Connection,
    output_dir: Path,
    filters: Sequence[str],
) -> int:
    """Move stale derivatives only after replacement views are durable."""

    filter_clause, parameters = _filter_sql(filters, alias="s")
    rows = connection.execute(
        f"""
        SELECT segment_id, output_path FROM {SEGMENT_TABLE} AS s
        WHERE status='stale'{filter_clause}
        ORDER BY segment_id
        """,
        parameters,
    ).fetchall()
    for row in rows:
        path = Path(str(row["output_path"]))
        if not path.is_file():
            continue
        quarantine = _quarantine_path(output_dir, str(row["segment_id"]), path)
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, quarantine)
    if rows:
        with connection:
            connection.executemany(
                f"""
                UPDATE {SEGMENT_TABLE}
                SET status='quarantined', updated_at=?
                WHERE segment_id=? AND status='stale'
                """,
                [(_now(), str(row["segment_id"])) for row in rows],
            )
    return len(rows)


def _load_unassigned_shards(
    connection: sqlite3.Connection,
    filters: Sequence[str],
    *,
    limit: int,
    show_progress: bool,
) -> list[TaskShard]:
    plan_token = _active_plan_token(connection)
    token_clause = "" if plan_token is None else " AND t.plan_token=?"
    parameters: list[object] = [] if plan_token is None else [plan_token]
    filter_clause, filter_parameters = _filter_sql(filters, alias="t")
    parameters.extend(filter_parameters)
    parameters.append(int(limit))
    cursor = connection.execute(
        f"""
        SELECT t.task_id, t.endpoint, t.output_path, t.rows, t.updated_at
        FROM tasks AS t
        LEFT JOIN {MEMBER_TABLE} AS m ON m.task_id=t.task_id
        WHERE t.active=1 AND t.status='success' AND m.task_id IS NULL
          {token_clause}{filter_clause}
        ORDER BY t.endpoint, t.task_id
        LIMIT ?
        """,
        parameters,
    )
    output: list[TaskShard] = []
    progress = tqdm(
        cursor,
        desc="openbb:l1 source contracts",
        unit="file",
        disable=not show_progress,
    )
    for row in progress:
        path = Path(str(row["output_path"])).resolve()
        stat = path.stat()
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        metadata_rows = int(metadata.num_rows)
        uncompressed_bytes = sum(
            int(metadata.row_group(index).total_byte_size)
            for index in range(metadata.num_row_groups)
        )
        manifest_rows = int(row["rows"])
        if metadata_rows != manifest_rows:
            raise RuntimeError(
                "successful source shard row mismatch: "
                f"task={row['task_id']} manifest={manifest_rows} "
                f"pyarrow={metadata_rows} path={path}"
            )
        output.append(
            TaskShard(
                task_id=str(row["task_id"]),
                endpoint=str(row["endpoint"]),
                output_path=str(path),
                rows=manifest_rows,
                task_updated_at=str(row["updated_at"]),
                bytes=int(stat.st_size),
                uncompressed_bytes=uncompressed_bytes,
                mtime_ns=int(stat.st_mtime_ns),
            )
        )
    return output


def _segment_batches(
    shards: Sequence[TaskShard],
    *,
    max_files: int,
    max_bytes: int,
    max_uncompressed_bytes: int,
    max_rows: int,
    min_files: int,
    include_tail: bool,
    flush_endpoints: set[str] | None = None,
) -> Iterator[list[TaskShard]]:
    forced = flush_endpoints or set()
    by_endpoint: dict[str, list[TaskShard]] = defaultdict(list)
    for shard in shards:
        by_endpoint[shard.endpoint].append(shard)
    for endpoint in sorted(by_endpoint):
        batch: list[TaskShard] = []
        bytes_in_batch = 0
        uncompressed_bytes_in_batch = 0
        rows_in_batch = 0
        for shard in by_endpoint[endpoint]:
            exceeds = batch and (
                len(batch) >= max_files
                or bytes_in_batch + shard.bytes > max_bytes
                or uncompressed_bytes_in_batch + shard.uncompressed_bytes
                > max_uncompressed_bytes
                or rows_in_batch + shard.rows > max_rows
            )
            if exceeds:
                # Resource targets are hard segment boundaries. A source file
                # itself is indivisible, but we never grow an existing batch
                # beyond a bound merely to satisfy the small-file threshold.
                yield batch
                batch = []
                bytes_in_batch = 0
                uncompressed_bytes_in_batch = 0
                rows_in_batch = 0
            batch.append(shard)
            bytes_in_batch += shard.bytes
            uncompressed_bytes_in_batch += shard.uncompressed_bytes
            rows_in_batch += shard.rows
            if len(batch) >= max_files:
                yield batch
                batch = []
                bytes_in_batch = 0
                uncompressed_bytes_in_batch = 0
                rows_in_batch = 0
        if batch and (
            len(batch) >= min_files or include_tail or endpoint in forced
        ):
            yield batch


def _segment_id(endpoint: str, signature: str) -> str:
    # The source signature already hashes all source identities and contracts.
    # Prefixing the endpoint makes collision diagnosis human-auditable.
    import hashlib

    return hashlib.sha256(f"{endpoint}\0{signature}".encode("utf-8")).hexdigest()[:24]


def _available_segment_id(
    connection: sqlite3.Connection, endpoint: str, signature: str
) -> str:
    base = _segment_id(endpoint, signature)
    candidate = base
    revision = 0
    while True:
        existing = connection.execute(
            f"SELECT status FROM {SEGMENT_TABLE} WHERE segment_id=?", (candidate,)
        ).fetchone()
        if existing is None:
            return candidate
        if existing["status"] == "success":
            raise RuntimeError(
                "source contract is already represented by an active segment: "
                f"{candidate}"
            )
        revision += 1
        candidate = f"{base}-r{revision}"


def _segment_output(output_dir: Path, endpoint: str, segment_id: str) -> Path:
    return (
        output_dir
        / "compact_l1"
        / Path(*endpoint.split("."))
        / "segments"
        / f"segment-{segment_id}.parquet"
    )


def _record_segment(
    connection: sqlite3.Connection,
    endpoint: str,
    segment_id: str,
    signature: str,
    shards: Sequence[TaskShard],
    receipt: CompactParquetReceipt,
) -> None:
    timestamp = _now()
    connection.execute("BEGIN IMMEDIATE")
    try:
        for shard in shards:
            current = connection.execute(
                """
                SELECT endpoint, output_path, rows, updated_at, status, active
                FROM tasks WHERE task_id=?
                """,
                (shard.task_id,),
            ).fetchone()
            if current is None or (
                current["status"] != "success"
                or int(current["active"] or 0) != 1
                or current["endpoint"] != endpoint
                or str(Path(str(current["output_path"])).resolve())
                != shard.output_path
                or int(current["rows"] or -1) != shard.rows
                or current["updated_at"] != shard.task_updated_at
            ):
                raise RuntimeError(
                    "source task changed while segment was being built: "
                    f"task={shard.task_id}"
                )
        connection.execute(
            f"""
            INSERT INTO {SEGMENT_TABLE} (
                segment_id, endpoint, output_path, source_signature,
                source_files, source_rows, source_bytes, output_rows,
                source_uncompressed_bytes, output_bytes, row_groups,
                schema_fingerprint, date_column,
                min_date, max_date, compression, status, stale_reason,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      'success', NULL, ?, ?)
            """,
            (
                segment_id,
                endpoint,
                receipt.output_path,
                signature,
                receipt.source_files,
                receipt.source_rows,
                receipt.source_bytes,
                receipt.output_rows,
                sum(shard.uncompressed_bytes for shard in shards),
                receipt.output_bytes,
                receipt.row_groups,
                receipt.schema_fingerprint,
                receipt.date_column,
                receipt.min_date,
                receipt.max_date,
                receipt.compression,
                timestamp,
                timestamp,
            ),
        )
        connection.executemany(
            f"""
            INSERT INTO {MEMBER_TABLE} (
                task_id, endpoint, segment_id, source_path, source_rows,
                source_bytes, source_uncompressed_bytes, source_mtime_ns,
                task_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    shard.task_id,
                    endpoint,
                    segment_id,
                    shard.output_path,
                    shard.rows,
                    shard.bytes,
                    shard.uncompressed_bytes,
                    shard.mtime_ns,
                    shard.task_updated_at,
                )
                for shard in shards
            ],
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _active_segment_paths(
    connection: sqlite3.Connection,
) -> dict[str, list[Path]]:
    grouped: dict[str, list[Path]] = defaultdict(list)
    for row in connection.execute(
        f"""
        SELECT endpoint, output_path FROM {SEGMENT_TABLE}
        WHERE status='success' ORDER BY endpoint, segment_id
        """
    ):
        grouped[str(row["endpoint"])].append(Path(str(row["output_path"])).resolve())
    return dict(grouped)


def _publish_views(connection: sqlite3.Connection, database_path: Path) -> int:
    grouped = _active_segment_paths(connection)
    names: dict[str, str] = {}
    for endpoint in grouped:
        name = _view_name(endpoint)
        if name in names and names[name] != endpoint:
            raise RuntimeError(
                f"DuckDB view collision: {endpoint!r} and {names[name]!r} -> {name}"
            )
        names[name] = endpoint
    database_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = database_path.with_name(f".{database_path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    database = duckdb.connect(str(temporary))
    try:
        database.execute(
            "CREATE TABLE l1_catalog ("
            "endpoint VARCHAR, view_name VARCHAR, segment_count BIGINT, "
            "source_files BIGINT, rows BIGINT, input_bytes BIGINT, "
            "output_bytes BIGINT, updated_at_utc VARCHAR)"
        )
        for endpoint, paths in grouped.items():
            missing = [str(path) for path in paths if not path.is_file()]
            if missing:
                raise FileNotFoundError(
                    f"active L1 segment files are missing: {missing[:5]}"
                )
            path_sql = "[" + ",".join(_sql_string(path) for path in paths) + "]"
            view_name = _view_name(endpoint)
            database.execute(
                f"CREATE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet({path_sql}, union_by_name=true)"
            )
            totals = connection.execute(
                f"""
                SELECT COUNT(*), SUM(source_files), SUM(output_rows),
                       SUM(source_bytes), SUM(output_bytes), MAX(updated_at)
                FROM {SEGMENT_TABLE}
                WHERE endpoint=? AND status='success'
                """,
                (endpoint,),
            ).fetchone()
            database.execute(
                "INSERT INTO l1_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    endpoint,
                    view_name,
                    int(totals[0] or 0),
                    int(totals[1] or 0),
                    int(totals[2] or 0),
                    int(totals[3] or 0),
                    int(totals[4] or 0),
                    str(totals[5] or ""),
                ),
            )
        database.execute("CHECKPOINT")
    finally:
        database.close()
    os.replace(temporary, database_path)
    return len(grouped)


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _grouped_counts(
    connection: sqlite3.Connection,
    sql: str,
    filters: Sequence[str],
    *,
    alias: str,
) -> dict[str, sqlite3.Row]:
    filter_clause, parameters = _filter_sql(filters, alias=alias)
    return {
        str(row["endpoint"]): row
        for row in connection.execute(sql.format(filter_clause=filter_clause), parameters)
    }


def _write_status(
    connection: sqlite3.Connection,
    output_dir: Path,
    filters: Sequence[str],
    *,
    stale_segments: int,
    new_segments: int,
) -> dict[str, object]:
    plan_token = _active_plan_token(connection)
    token_clause = "" if plan_token is None else " AND t.plan_token=?"
    filter_clause, filter_parameters = _filter_sql(filters, alias="t")
    task_parameters: list[object] = [] if plan_token is None else [plan_token]
    task_parameters.extend(filter_parameters)
    tasks = {
        str(row["endpoint"]): row
        for row in connection.execute(
            f"""
            SELECT t.endpoint, COUNT(*) AS success_files,
                   SUM(t.rows) AS success_rows
            FROM tasks AS t
            WHERE t.active=1 AND t.status='success'{token_clause}{filter_clause}
            GROUP BY t.endpoint
            """,
            task_parameters,
        )
    }
    compacted = _grouped_counts(
        connection,
        f"""
        SELECT m.endpoint, COUNT(*) AS compacted_files,
               SUM(m.source_rows) AS compacted_rows
        FROM {MEMBER_TABLE} AS m
        JOIN {SEGMENT_TABLE} AS s ON s.segment_id=m.segment_id
        WHERE s.status='success'{{filter_clause}}
        GROUP BY m.endpoint
        """,
        filters,
        alias="m",
    )
    segments = _grouped_counts(
        connection,
        f"""
        SELECT s.endpoint, COUNT(*) AS active_segments,
               SUM(s.source_bytes) AS source_bytes,
               SUM(s.output_bytes) AS output_bytes,
               MAX(s.updated_at) AS latest_segment_at_utc
        FROM {SEGMENT_TABLE} AS s
        WHERE s.status='success'{{filter_clause}}
        GROUP BY s.endpoint
        """,
        filters,
        alias="s",
    )
    endpoint_rows: list[dict[str, object]] = []
    for endpoint in sorted(set(tasks) | set(compacted) | set(segments)):
        task = tasks.get(endpoint)
        compact = compacted.get(endpoint)
        segment = segments.get(endpoint)
        success_files = int(task["success_files"] or 0) if task is not None else 0
        success_rows = int(task["success_rows"] or 0) if task is not None else 0
        compacted_files = (
            int(compact["compacted_files"] or 0) if compact is not None else 0
        )
        compacted_rows = (
            int(compact["compacted_rows"] or 0) if compact is not None else 0
        )
        source_bytes = int(segment["source_bytes"] or 0) if segment is not None else 0
        output_bytes = int(segment["output_bytes"] or 0) if segment is not None else 0
        endpoint_rows.append(
            {
                "endpoint": endpoint,
                "view_name": _view_name(endpoint) if segment is not None else None,
                "success_files": success_files,
                "success_rows": success_rows,
                "compacted_files": compacted_files,
                "compacted_rows": compacted_rows,
                "pending_files": max(0, success_files - compacted_files),
                "pending_rows": max(0, success_rows - compacted_rows),
                "active_segments": int(segment["active_segments"] or 0)
                if segment is not None
                else 0,
                "source_bytes": source_bytes,
                "output_bytes": output_bytes,
                "space_reduction_fraction": round(
                    1.0 - (output_bytes / source_bytes), 8
                )
                if source_bytes
                else None,
                "latest_segment_at_utc": str(segment["latest_segment_at_utc"])
                if segment is not None
                else None,
            }
        )
    generated_at = _now()
    frame = (
        pl.DataFrame(endpoint_rows, infer_schema_length=None)
        if endpoint_rows
        else pl.DataFrame(
            schema={
                "endpoint": pl.String,
                "view_name": pl.String,
                "success_files": pl.Int64,
                "success_rows": pl.Int64,
                "compacted_files": pl.Int64,
                "compacted_rows": pl.Int64,
                "pending_files": pl.Int64,
                "pending_rows": pl.Int64,
                "active_segments": pl.Int64,
                "source_bytes": pl.Int64,
                "output_bytes": pl.Int64,
                "space_reduction_fraction": pl.Float64,
                "latest_segment_at_utc": pl.String,
            }
        )
    )
    status_path = output_dir / "catalog" / "l1_compaction_status.parquet"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = status_path.with_name(f".{status_path.name}.{os.getpid()}.tmp")
    try:
        frame.write_parquet(
            temporary, compression="zstd", compression_level=6, statistics=True
        )
        os.replace(temporary, status_path)
    finally:
        temporary.unlink(missing_ok=True)
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "active_plan_token": plan_token,
        "endpoint_filters": list(filters),
        "endpoints": len(endpoint_rows),
        "success_files": sum(int(row["success_files"]) for row in endpoint_rows),
        "success_rows": sum(int(row["success_rows"]) for row in endpoint_rows),
        "compacted_files": sum(int(row["compacted_files"]) for row in endpoint_rows),
        "compacted_rows": sum(int(row["compacted_rows"]) for row in endpoint_rows),
        "pending_files": sum(int(row["pending_files"]) for row in endpoint_rows),
        "pending_rows": sum(int(row["pending_rows"]) for row in endpoint_rows),
        "active_segments": sum(int(row["active_segments"]) for row in endpoint_rows),
        "source_bytes": sum(int(row["source_bytes"]) for row in endpoint_rows),
        "output_bytes": sum(int(row["output_bytes"]) for row in endpoint_rows),
        "new_segments": int(new_segments),
        "stale_segments": int(stale_segments),
        "l0_deleted": False,
        "query_database": str((output_dir / "openbb_l1.duckdb").resolve()),
        "status_parquet": str(status_path.resolve()),
        "endpoint_status": endpoint_rows,
    }
    _atomic_json(output_dir / "_state" / "l1_compaction_latest.json", payload)
    return payload


def _audit_segments(
    connection: sqlite3.Connection,
    output_dir: Path,
    filters: Sequence[str],
    *,
    show_progress: bool,
) -> list[SegmentAudit]:
    filter_clause, parameters = _filter_sql(filters, alias="s")
    segments = connection.execute(
        f"""
        SELECT * FROM {SEGMENT_TABLE} AS s
        WHERE s.status='success'{filter_clause}
        ORDER BY s.endpoint, s.segment_id
        """,
        parameters,
    ).fetchall()
    database_path = output_dir / "openbb_l1.duckdb"
    database = (
        duckdb.connect(str(database_path), read_only=True)
        if database_path.is_file()
        else None
    )
    view_rows: dict[str, int] = {}
    audits: list[SegmentAudit] = []
    progress = tqdm(
        segments,
        desc="openbb:l1 deep audit",
        unit="segment",
        disable=not show_progress,
    )
    try:
        for segment in progress:
            status = "passed"
            error: str | None = None
            arrow_rows = polars_rows = duckdb_rows = None
            endpoint = str(segment["endpoint"])
            output_path = Path(str(segment["output_path"]))
            try:
                members = connection.execute(
                    f"""
                    SELECT m.*, t.active, t.status AS task_status,
                           t.endpoint AS task_endpoint,
                           t.output_path AS task_output_path,
                           t.rows AS task_rows, t.updated_at AS current_task_updated_at
                    FROM {MEMBER_TABLE} AS m
                    LEFT JOIN tasks AS t ON t.task_id=m.task_id
                    WHERE m.segment_id=? ORDER BY m.task_id
                    """,
                    (segment["segment_id"],),
                ).fetchall()
                if len(members) != int(segment["source_files"]):
                    raise RuntimeError(
                        f"member count mismatch: expected={segment['source_files']} "
                        f"actual={len(members)}"
                    )
                source_rows = 0
                contracts: list[SourceFileContract] = []
                for member in members:
                    if (
                        member["task_status"] != "success"
                        or int(member["active"] or 0) != 1
                        or member["task_endpoint"] != member["endpoint"]
                        or str(Path(str(member["task_output_path"])).resolve())
                        != member["source_path"]
                        or int(member["task_rows"] or -1) != int(member["source_rows"])
                        or member["current_task_updated_at"]
                        != member["task_updated_at"]
                    ):
                        raise RuntimeError(
                            f"source manifest contract changed: task={member['task_id']}"
                        )
                    source_path = Path(str(member["source_path"]))
                    stat = source_path.stat()
                    source_file_rows = int(pq.ParquetFile(source_path).metadata.num_rows)
                    source_metadata = pq.ParquetFile(source_path).metadata
                    source_uncompressed_bytes = sum(
                        int(source_metadata.row_group(index).total_byte_size)
                        for index in range(source_metadata.num_row_groups)
                    )
                    if (
                        stat.st_size != int(member["source_bytes"])
                        or stat.st_mtime_ns != int(member["source_mtime_ns"])
                        or source_file_rows != int(member["source_rows"])
                    ):
                        raise RuntimeError(
                            f"source file contract changed: task={member['task_id']}"
                        )
                    recorded_uncompressed = int(
                        member["source_uncompressed_bytes"] or 0
                    )
                    if (
                        recorded_uncompressed > 0
                        and source_uncompressed_bytes != recorded_uncompressed
                    ):
                        raise RuntimeError(
                            "source uncompressed-size contract changed: "
                            f"task={member['task_id']}"
                        )
                    source_rows += source_file_rows
                    contracts.append(
                        SourceFileContract(
                            source_id=(
                                f"{member['task_id']}\0{member['task_updated_at']}"
                            ),
                            path=str(source_path),
                            rows=source_file_rows,
                            bytes=int(stat.st_size),
                            mtime_ns=int(stat.st_mtime_ns),
                        )
                    )
                if source_rows != int(segment["source_rows"]):
                    raise RuntimeError(
                        f"source row mismatch: expected={segment['source_rows']} "
                        f"actual={source_rows}"
                    )
                if source_signature(contracts) != segment["source_signature"]:
                    raise RuntimeError("source signature mismatch")
                arrow_rows = int(pq.ParquetFile(output_path).metadata.num_rows)
                polars_rows = int(
                    pl.scan_parquet(output_path)
                    .select(pl.len())
                    .collect(engine="streaming")
                    .item()
                )
                with duckdb.connect(":memory:") as verifier:
                    duckdb_rows = int(
                        verifier.execute(
                            "SELECT COUNT(*) FROM read_parquet(?)", [str(output_path)]
                        ).fetchone()[0]
                    )
                if not (
                    arrow_rows
                    == polars_rows
                    == duckdb_rows
                    == source_rows
                    == int(segment["output_rows"])
                ):
                    raise RuntimeError(
                        "output row mismatch: "
                        f"source={source_rows} recorded={segment['output_rows']} "
                        f"arrow={arrow_rows} polars={polars_rows} duckdb={duckdb_rows}"
                    )
                if parquet_schema_fingerprint(output_path) != segment[
                    "schema_fingerprint"
                ]:
                    raise RuntimeError("output schema fingerprint mismatch")
                if database is None:
                    raise RuntimeError("openbb_l1.duckdb is missing")
                if endpoint not in view_rows:
                    view_rows[endpoint] = int(
                        database.execute(
                            f"SELECT COUNT(*) FROM {_view_name(endpoint)}"
                        ).fetchone()[0]
                    )
                    expected_view_rows = int(
                        connection.execute(
                            f"""
                            SELECT SUM(output_rows) FROM {SEGMENT_TABLE}
                            WHERE endpoint=? AND status='success'
                            """,
                            (endpoint,),
                        ).fetchone()[0]
                        or 0
                    )
                    if view_rows[endpoint] != expected_view_rows:
                        raise RuntimeError(
                            "DuckDB endpoint view row mismatch: "
                            f"expected={expected_view_rows} actual={view_rows[endpoint]}"
                        )
            except Exception as exc:
                status = "failed"
                error = f"{type(exc).__name__}: {str(exc)[:4000]}"
            audits.append(
                SegmentAudit(
                    segment_id=str(segment["segment_id"]),
                    endpoint=endpoint,
                    status=status,
                    source_files=int(segment["source_files"]),
                    source_rows=int(segment["source_rows"]),
                    output_rows=int(segment["output_rows"]),
                    output_path=str(output_path),
                    arrow_rows=arrow_rows,
                    polars_rows=polars_rows,
                    duckdb_rows=duckdb_rows,
                    error=error,
                    checked_at_utc=_now(),
                )
            )
    finally:
        if database is not None:
            database.close()
    return audits


def _write_audit(output_dir: Path, rows: Sequence[SegmentAudit]) -> None:
    path = output_dir / "catalog" / "l1_compaction_audit.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = (
        pl.DataFrame([asdict(row) for row in rows], infer_schema_length=None)
        if rows
        else pl.DataFrame(
            schema={
                "segment_id": pl.String,
                "endpoint": pl.String,
                "status": pl.String,
                "source_files": pl.Int64,
                "source_rows": pl.Int64,
                "output_rows": pl.Int64,
                "output_path": pl.String,
                "arrow_rows": pl.Int64,
                "polars_rows": pl.Int64,
                "duckdb_rows": pl.Int64,
                "error": pl.String,
                "checked_at_utc": pl.String,
            }
        )
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.write_parquet(
            temporary, compression="zstd", compression_level=6, statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()
    state_dir = output_dir / "_state"
    state_path = state_dir / "openbb_archive.sqlite3"
    lock_path = state_dir / "openbb_l1_compaction.lock"
    with _exclusive_lock(lock_path):
        manifest = _open_manifest(state_path)
        try:
            if args.audit_only:
                audits = _audit_segments(
                    manifest,
                    output_dir,
                    args.endpoint,
                    show_progress=not args.no_progress,
                )
                _write_audit(output_dir, audits)
                failed = sum(row.status == "failed" for row in audits)
                print(
                    "[openbb-l1-audit] "
                    f"segments={len(audits)} passed={len(audits) - failed} "
                    f"failed={failed}",
                    flush=True,
                )
                return 0 if failed == 0 else 2

            stale_endpoints, stale_segments = _mark_stale_segments(
                manifest, args.endpoint
            )
            shards = _load_unassigned_shards(
                manifest,
                args.endpoint,
                limit=args.max_source_files,
                show_progress=not args.no_progress,
            )
            batches = list(
                _segment_batches(
                    shards,
                    max_files=args.max_files_per_segment,
                    max_bytes=args.max_source_bytes_per_segment,
                    max_uncompressed_bytes=(
                        args.max_uncompressed_bytes_per_segment
                    ),
                    max_rows=args.max_source_rows_per_segment,
                    min_files=args.min_files_per_segment,
                    include_tail=args.include_tail,
                    flush_endpoints=stale_endpoints,
                )
            )
            progress = tqdm(
                batches,
                desc="openbb:l1 compact",
                unit="segment",
                disable=args.no_progress,
            )
            new_segments = 0
            for batch in progress:
                endpoint = batch[0].endpoint
                contracts = [item.source_contract() for item in batch]
                signature = source_signature(contracts)
                segment_id = _available_segment_id(manifest, endpoint, signature)
                output_path = _segment_output(output_dir, endpoint, segment_id)
                receipt = compact_parquet_files(
                    [item.output_path for item in batch],
                    output_path,
                    expected_rows=sum(item.rows for item in batch),
                    threads=args.threads,
                    memory_limit=args.memory_limit,
                    compression="zstd",
                    row_group_size_rows=args.row_group_size,
                    temp_directory=state_dir / "duckdb_l1_tmp",
                )
                try:
                    _record_segment(
                        manifest,
                        endpoint,
                        segment_id,
                        signature,
                        batch,
                        receipt,
                    )
                except Exception:
                    # Publication reached the filesystem but not the manifest.
                    # Preserve the file for diagnosis instead of deleting data.
                    orphan = _quarantine_path(output_dir, segment_id, output_path)
                    orphan.parent.mkdir(parents=True, exist_ok=True)
                    if output_path.exists():
                        os.replace(output_path, orphan)
                    raise
                new_segments += 1
                progress.set_postfix(
                    endpoint=endpoint,
                    files=receipt.source_files,
                    rows=receipt.output_rows,
                    refresh=False,
                )

            view_count = _publish_views(manifest, output_dir / "openbb_l1.duckdb")
            quarantined_segments = _quarantine_stale_outputs(
                manifest, output_dir, args.endpoint
            )
            status = _write_status(
                manifest,
                output_dir,
                (),
                stale_segments=stale_segments,
                new_segments=new_segments,
            )
            print(
                "[openbb-l1] "
                f"new_segments={new_segments} stale_segments={stale_segments} "
                f"quarantined_segments={quarantined_segments} "
                f"views={view_count} compacted_files={status['compacted_files']} "
                f"pending_files={status['pending_files']} l0_deleted=false",
                flush=True,
            )
            return 0
        finally:
            manifest.close()


if __name__ == "__main__":
    raise SystemExit(run())
