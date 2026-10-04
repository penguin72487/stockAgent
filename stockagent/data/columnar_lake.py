"""Offline columnar compaction; not a panel or training-runtime backend."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import os
from pathlib import Path
import stat
from typing import Callable, Iterable, Sequence

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


def read_daily_projection(
    source: str | Path, *, columns: Sequence[str], symbols: Sequence[str],
    start_date: date, end_date: date | None = None, engine: str = 'arrow',
    duckdb_threads: int = 4, duckdb_memory_limit: str = '1GB',
):
    """Read actual daily observations with projection and predicate pushdown.

    This is the existing offline columnar owner's bounded analysis API, not a
    panel backend or eligibility clock. Callers select an immutable source and
    retain its identity. Engine choice is explicit; measured query timings do
    not change training defaults. Identifiers are quoted and values bound.
    """
    import pyarrow as pa
    import pyarrow.dataset as ds
    source=Path(source).resolve(strict=True)
    if engine not in ('arrow','polars','duckdb'):
        raise ValueError('unknown columnar query engine')
    if type(duckdb_threads) is not int or not 1<=duckdb_threads<=1024:
        raise ValueError('query threads must be a positive bounded integer')
    if type(start_date) is not date or (end_date is not None and type(end_date) is not date):
        raise ValueError('daily query needs explicit calendar dates')
    if end_date is not None and end_date<start_date:
        raise ValueError('daily query date range is reversed')
    if not columns or len(set(columns))!=len(columns) or any(not isinstance(c,str) or not c or '\x00' in c for c in columns):
        raise ValueError('query columns must be distinct names')
    if not symbols or any(not isinstance(s,str) or not s for s in symbols):
        raise ValueError('symbol identity must remain non-empty strings')
    dataset=ds.dataset(source,format='parquet',partitioning='hive')
    schema=dataset.schema
    if 'date' not in schema.names or not pa.types.is_date(schema.field('date').type):
        raise ValueError('daily query cannot reinterpret a timestamp or text as an observation date')
    for name in (*columns,'symbol'):
        if name not in schema.names:raise ValueError('source column missing: '+name)
    if engine=='arrow':
        predicate=(ds.field('date')>=start_date)&ds.field('symbol').isin(symbols)
        if end_date is not None:predicate=predicate&(ds.field('date')<=end_date)
        result=dataset.to_table(columns=list(columns),filter=predicate)
    elif engine=='polars':
        path=str(source/'**/*.parquet') if source.is_dir() else str(source)
        predicate=(pl.col('date')>=start_date)&pl.col('symbol').is_in(symbols)
        if end_date is not None:predicate=predicate&(pl.col('date')<=end_date)
        result=pl.scan_parquet(path).filter(predicate).select(list(columns)).collect().to_arrow()
    else:
        import duckdb
        path=str(source/'**/*.parquet') if source.is_dir() else str(source)
        names=','.join('"'+name.replace('"','""')+'"' for name in columns)
        sql='SELECT '+names+' FROM read_parquet(?) WHERE "date" >= ? AND "symbol" IN ('+','.join('?' for _ in symbols)+')'
        parameters=[path,start_date,*symbols]
        if end_date is not None:sql+=' AND "date" <= ?';parameters.append(end_date)
        with duckdb.connect() as connection:
            connection.execute('SET threads='+str(duckdb_threads))
            connection.execute('SET memory_limit='+_duckdb_string(duckdb_memory_limit))
            result=connection.execute(sql,parameters).to_arrow_table()
    return result.cast(pa.schema([schema.field(name) for name in columns]))


@dataclass(frozen=True, slots=True)
class SourceFileContract:
    source_id: str
    path: str
    rows: int
    bytes: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class SourceMetadataProof:
    """Ephemeral, same-run footer observation; never a persisted cache contract."""

    path: str
    rows: int
    bytes: int
    uncompressed_bytes: int
    schema_fingerprint: str
    file_signature: tuple[int, int, int, int, int]


def source_file_signature(path: str | Path) -> tuple[int, int, int, int, int]:
    info = Path(path).stat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f"source is not a regular file: {path}")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def observe_source_metadata(path: str | Path) -> SourceMetadataProof:
    """Read one footer inside an identity fence, retaining only scalar evidence."""
    resolved = Path(path).resolve()
    before = source_file_signature(resolved)
    with pq.ParquetFile(resolved) as parquet_file:
        metadata = parquet_file.metadata
        rows = int(metadata.num_rows)
        uncompressed = sum(
            int(metadata.row_group(index).total_byte_size)
            for index in range(metadata.num_row_groups)
        )
        schema = parquet_file.schema_arrow.remove_metadata()
        fingerprint = hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
    if source_file_signature(resolved) != before:
        raise RuntimeError(f"source changed during metadata observation: {resolved}")
    return SourceMetadataProof(
        str(resolved), rows, before[2], uncompressed, fingerprint, before
    )


def verify_source_metadata_proofs(
    paths: Sequence[str | Path], proofs: Sequence[SourceMetadataProof],
) -> None:
    """Reject missing/substituted/stale same-run evidence without reopening footers."""
    if len(paths) != len(proofs):
        raise RuntimeError("source metadata proof count mismatch")
    for path, proof in zip(paths, proofs, strict=True):
        # The same-run loader and compactor already canonicalized these paths.
        # stat still follows replacements; avoid re-walking every parent at
        # each fence when the lexical path already matches the observation.
        resolved = Path(path)
        if not isinstance(proof, SourceMetadataProof) or str(resolved) != proof.path:
            resolved = resolved.resolve()
        if not isinstance(proof, SourceMetadataProof) or (
            str(resolved) != proof.path
            or type(proof.rows) is not int or proof.rows < 0
            or type(proof.bytes) is not int or proof.bytes < 0
            or type(proof.uncompressed_bytes) is not int or proof.uncompressed_bytes < 0
            or len(proof.file_signature) != 5
            or any(type(value) is not int for value in proof.file_signature)
            or proof.bytes != proof.file_signature[2]
            or len(proof.schema_fingerprint) != 64
            or any(value not in "0123456789abcdef" for value in proof.schema_fingerprint)
        ):
            raise RuntimeError(f"invalid source metadata proof: {resolved}")
        if source_file_signature(resolved) != proof.file_signature:
            raise RuntimeError(f"source changed since metadata observation: {resolved}")


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
    source_metadata_proofs: Sequence[SourceMetadataProof] | None = None,
    before_publish: Callable[[], None] | None = None,
) -> CompactParquetReceipt:
    """Atomically compact Parquet files and validate with three independent readers.

    The function never deletes inputs.  PyArrow metadata establishes the source
    row contract, DuckDB performs bounded-memory union-by-name compaction, and
    PyArrow, Polars, and a fresh DuckDB query must agree before publication.
    """

    # Metadata/receipt helpers must not load this optional offline SQL engine.
    import duckdb

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

    proofs = None if source_metadata_proofs is None else tuple(source_metadata_proofs)
    if proofs is None:
        source_rows = 0
        source_bytes = 0
        for path in paths:
            source_rows += int(pq.ParquetFile(path).metadata.num_rows)
            source_bytes += int(path.stat().st_size)
    else:
        verify_source_metadata_proofs(paths, proofs)
        source_rows = sum(proof.rows for proof in proofs)
        source_bytes = sum(proof.bytes for proof in proofs)
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
        if proofs is not None:
            verify_source_metadata_proofs(paths, proofs)

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
        if before_publish is not None:
            before_publish()
        if proofs is not None:
            verify_source_metadata_proofs(paths, proofs)
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
