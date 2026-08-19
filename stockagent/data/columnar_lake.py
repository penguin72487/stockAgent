from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
import polars as pl
import pyarrow.parquet as pq


DATE_COLUMN_CANDIDATES = (
    "date",
    "timestamp",
    "ts",
    "open_time",
    "filing_date",
    "period_ending",
    "report_date",
    "published_date",
    "updated_at",
)


@dataclass(frozen=True, slots=True)
class SourceFileContract:
    source_id: str
    path: str
    rows: int
    bytes: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class CompactParquetReceipt:
    schema_version: int
    source_files: int
    source_rows: int
    source_bytes: int
    output_path: str
    output_rows: int
    output_bytes: int
    row_groups: int
    schema_fingerprint: str
    date_column: str | None
    min_date: str | None
    max_date: str | None
    compression: str
    compression_level: int | None
    row_group_size_rows: int
    pyarrow_rows: int
    polars_rows: int
    duckdb_rows: int
    generated_at_utc: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def source_signature(contracts: Iterable[SourceFileContract]) -> str:
    """Hash the complete ordered source contract without reading secret data."""

    digest = hashlib.sha256()
    for item in sorted(contracts, key=lambda value: value.source_id):
        for value in (
            item.source_id,
            item.path,
            str(item.rows),
            str(item.bytes),
            str(item.mtime_ns),
        ):
            encoded = value.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, byteorder="little", signed=False))
            digest.update(encoded)
    return digest.hexdigest()


def parquet_schema_fingerprint(path: str | Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow.remove_metadata()
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _fsync_path(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _duckdb_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _polars_summary(
    path: Path,
) -> tuple[int, str | None, str | None, str | None]:
    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    date_column = next(
        (name for name in DATE_COLUMN_CANDIDATES if name in schema),
        None,
    )
    expressions: list[pl.Expr] = [pl.len().alias("rows")]
    if date_column is not None:
        expressions.extend(
            [
                pl.col(date_column)
                .cast(pl.String, strict=False)
                .min()
                .alias("min_date"),
                pl.col(date_column)
                .cast(pl.String, strict=False)
                .max()
                .alias("max_date"),
            ]
        )
    row = scan.select(expressions).collect(engine="streaming").row(0, named=True)
    return (
        int(row["rows"]),
        date_column,
        str(row["min_date"]) if row.get("min_date") is not None else None,
        str(row["max_date"]) if row.get("max_date") is not None else None,
    )


def compact_parquet_files(
    source_paths: Sequence[str | Path],
    output_path: str | Path,
    *,
    expected_rows: int | None = None,
    threads: int = 4,
    memory_limit: str = "4GB",
    compression: str = "zstd",
    compression_level: int | None = 3,
    row_group_size_rows: int = 122_880,
    temp_directory: str | Path | None = None,
) -> CompactParquetReceipt:
    """Atomically compact Parquet files and validate with three independent readers.

    The function never deletes inputs.  PyArrow metadata establishes the source
    row contract, DuckDB performs bounded-memory union-by-name compaction, and
    PyArrow, Polars, and a fresh DuckDB query must agree before publication.
    """

    paths = [Path(path).resolve() for path in source_paths]
    codec = str(compression).strip().lower()
    if codec not in {"zstd", "snappy", "gzip", "lz4", "uncompressed"}:
        raise ValueError(f"unsupported Parquet compression codec: {compression!r}")
    if compression_level is not None:
        if codec != "zstd":
            raise ValueError("compression_level is supported only for zstd")
        if not 1 <= int(compression_level) <= 22:
            raise ValueError("zstd compression_level must be between 1 and 22")
    if not paths:
        raise ValueError("at least one source Parquet file is required")
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"source Parquet files are missing: {missing[:5]}")

    source_rows = 0
    source_bytes = 0
    for path in paths:
        source_rows += int(pq.ParquetFile(path).metadata.num_rows)
        source_bytes += int(path.stat().st_size)
    if expected_rows is not None and source_rows != int(expected_rows):
        raise RuntimeError(
            "source row-count mismatch: "
            f"manifest={int(expected_rows)} pyarrow_metadata={source_rows}"
        )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    duckdb_temp = (
        Path(temp_directory)
        if temp_directory is not None
        else target.parent / ".duckdb_tmp"
    )
    duckdb_temp.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"SET threads={max(1, int(threads))}")
        connection.execute(f"SET memory_limit={_duckdb_string(memory_limit)}")
        # L1 fact semantics never depend on the arbitrary input shard order.
        # Disabling order preservation lets parallel COPY stream row groups
        # instead of spilling large reorder buffers for thousands of files.
        connection.execute("SET preserve_insertion_order=false")
        connection.execute(
            f"SET temp_directory={_duckdb_string(duckdb_temp.resolve())}"
        )
        relation = connection.read_parquet(
            [str(path) for path in paths],
            union_by_name=True,
        )
        relation.create_view("_columnar_lake_source", replace=True)
        level_option = (
            ""
            if compression_level is None
            else f", COMPRESSION_LEVEL {int(compression_level)}"
        )
        connection.execute(
            "COPY (SELECT * FROM _columnar_lake_source) "
            f"TO {_duckdb_string(temporary)} "
            f"(FORMAT parquet, COMPRESSION {codec}{level_option}, "
            f"ROW_GROUP_SIZE {max(1, int(row_group_size_rows))})"
        )

        metadata = pq.ParquetFile(temporary).metadata
        arrow_rows = int(metadata.num_rows)
        polars_rows, date_column, min_date, max_date = _polars_summary(temporary)
        duckdb_rows = int(
            connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(temporary)]
            ).fetchone()[0]
        )
        if not arrow_rows == polars_rows == duckdb_rows == source_rows:
            raise RuntimeError(
                "compacted row-count mismatch: "
                f"source={source_rows} pyarrow={arrow_rows} "
                f"polars={polars_rows} duckdb={duckdb_rows}"
            )

        _fsync_path(temporary)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
        output_metadata = pq.ParquetFile(target).metadata
        return CompactParquetReceipt(
            schema_version=1,
            source_files=len(paths),
            source_rows=source_rows,
            source_bytes=source_bytes,
            output_path=str(target),
            output_rows=int(output_metadata.num_rows),
            output_bytes=int(target.stat().st_size),
            row_groups=int(output_metadata.num_row_groups),
            schema_fingerprint=parquet_schema_fingerprint(target),
            date_column=date_column,
            min_date=min_date,
            max_date=max_date,
            compression=codec,
            compression_level=compression_level,
            row_group_size_rows=max(1, int(row_group_size_rows)),
            pyarrow_rows=arrow_rows,
            polars_rows=polars_rows,
            duckdb_rows=duckdb_rows,
            generated_at_utc=datetime.now(timezone.utc).isoformat(),
        )
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)
