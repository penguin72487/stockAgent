from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import duckdb
import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm


DATE_COLUMN_CANDIDATES = (
    "date",
    "filing_date",
    "period_ending",
    "report_date",
    "published_date",
    "updated_at",
)


@dataclass(slots=True)
class CompactResult:
    endpoint: str
    status: str
    source_files: int
    source_rows: int
    output_rows: int
    output_path: str | None
    min_date: str | None = None
    max_date: str | None = None
    error: str | None = None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compact OpenBB task shards with DuckDB, validate them with Polars/PyArrow, "
            "and publish query views in data_openBB/openbb.duckdb."
        )
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_openBB"))
    parser.add_argument(
        "--endpoint",
        action="append",
        default=[],
        help="Endpoint or prefix filter; repeatable",
    )
    parser.add_argument(
        "--threads", type=int, default=max(1, min(8, os.cpu_count() or 1))
    )
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help=(
            "Do not rewrite compact files; independently verify source signatures, "
            "manifest/PyArrow/Polars row counts, and DuckDB views."
        ),
    )
    parser.add_argument("--no-progress", action="store_true")
    return parser.parse_args(argv)


def _endpoint_matches(endpoint: str, filters: Sequence[str]) -> bool:
    cleaned = [item.strip().lstrip(".") for item in filters if item.strip()]
    return not cleaned or any(
        endpoint == item or endpoint.startswith(f"{item}.") for item in cleaned
    )


def _open_manifest(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"OpenBB manifest does not exist: {path}")
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS compactions (
            endpoint TEXT PRIMARY KEY,
            source_signature TEXT NOT NULL,
            source_files INTEGER NOT NULL,
            source_rows INTEGER NOT NULL,
            output_path TEXT,
            output_rows INTEGER NOT NULL DEFAULT 0,
            min_date TEXT,
            max_date TEXT,
            status TEXT NOT NULL,
            error TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def _active_plan_token(connection: sqlite3.Connection) -> str | None:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive_meta'"
    ).fetchone()
    if table is None:
        return None
    row = connection.execute(
        "SELECT value FROM archive_meta WHERE key='active_plan_token'"
    ).fetchone()
    return str(row[0]) if row is not None else None


def _source_rows(
    connection: sqlite3.Connection,
    filters: Sequence[str],
    *,
    show_progress: bool = False,
) -> dict[str, list[sqlite3.Row]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    plan_token = _active_plan_token(connection)
    token_clause = "" if plan_token is None else " AND plan_token=?"
    parameters: tuple[str, ...] = () if plan_token is None else (plan_token,)
    total = int(
        connection.execute(
            f"SELECT COUNT(*) FROM tasks WHERE active=1 AND status='success'{token_clause}",
            parameters,
        ).fetchone()[0]
    )
    progress = tqdm(
        total=total,
        desc="compact:scan manifest",
        unit="task",
        disable=not show_progress,
    )
    cursor = connection.execute(
        """
        SELECT task_id, endpoint, output_path, rows, updated_at
        FROM tasks
        WHERE active=1 AND status='success'{token_clause}
        ORDER BY endpoint, task_id
        """.format(token_clause=token_clause),
        parameters,
    )
    try:
        for row in cursor:
            endpoint = str(row["endpoint"])
            if _endpoint_matches(endpoint, filters):
                # Do not silently omit a successful manifest row whose shard
                # disappeared. The pre-compaction source audit should catch
                # this first, and standalone compaction must fail closed too.
                grouped.setdefault(endpoint, []).append(row)
            progress.update(1)
            if progress.n % 5000 == 0:
                progress.set_postfix(
                    endpoints=len(grouped),
                    files=sum(map(len, grouped.values())),
                    refresh=False,
                )
    finally:
        progress.close()
    return grouped


def _source_signature(
    rows: Sequence[sqlite3.Row],
    *,
    endpoint: str = "",
    show_progress: bool = False,
) -> str:
    digest = hashlib.sha256()
    progress = tqdm(
        rows,
        total=len(rows),
        desc=f"compact:files:{endpoint}"[:64],
        unit="file",
        position=2,
        leave=False,
        miniters=max(1, min(1000, len(rows) // 100 if rows else 1)),
        disable=not show_progress,
    )
    try:
        for row in progress:
            path = Path(str(row["output_path"]))
            stat = path.stat()
            digest.update(str(row["task_id"]).encode("utf-8"))
            digest.update(str(row["rows"]).encode("ascii"))
            digest.update(str(stat.st_size).encode("ascii"))
            digest.update(str(stat.st_mtime_ns).encode("ascii"))
    finally:
        progress.close()
    return digest.hexdigest()


def _duckdb_view_name(endpoint: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_]+", "_", endpoint).strip("_").lower()
    return f"openbb_{value}"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _polars_validate(path: Path) -> tuple[int, str | None, str | None, str | None]:
    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    date_column = next(
        (name for name in DATE_COLUMN_CANDIDATES if name in schema), None
    )
    if date_column is None:
        rows = int(scan.select(pl.len()).collect(engine="streaming").item())
        return rows, None, None, None
    summary = (
        scan.select(
            pl.len().alias("rows"),
            pl.col(date_column).cast(pl.String, strict=False).min().alias("min_date"),
            pl.col(date_column).cast(pl.String, strict=False).max().alias("max_date"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return int(summary["rows"]), date_column, summary["min_date"], summary["max_date"]


def _validate_compacted_output(
    path: Path, expected_rows: int
) -> tuple[int, str | None, str | None]:
    """Read one compact output through both engines and enforce row parity."""
    arrow_rows = int(pq.ParquetFile(path).metadata.num_rows)
    polars_rows, _, min_date, max_date = _polars_validate(path)
    if arrow_rows != polars_rows or polars_rows != int(expected_rows):
        raise RuntimeError(
            "row-count mismatch: "
            f"manifest={expected_rows} pyarrow={arrow_rows} polars={polars_rows}"
        )
    return polars_rows, min_date, max_date


def _write_compaction_audit(rows: Sequence[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(list(rows), infer_schema_length=None)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.write_parquet(
            temporary, compression="zstd", compression_level=6, statistics=True
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _audit_compactions(
    manifest: sqlite3.Connection,
    grouped: dict[str, list[sqlite3.Row]],
    database_path: Path,
    *,
    show_progress: bool,
) -> list[dict[str, object]]:
    """Independently prove every active successful endpoint is query-complete."""
    checked_at = datetime.now(timezone.utc).isoformat()
    database = (
        duckdb.connect(str(database_path), read_only=True)
        if database_path.is_file()
        else None
    )
    audit_rows: list[dict[str, object]] = []
    progress = tqdm(
        grouped.items(),
        total=len(grouped),
        desc="openbb:compact audit",
        unit="endpoint",
        disable=not show_progress,
    )
    try:
        for endpoint, source_rows in progress:
            source_files = len(source_rows)
            expected_rows = sum(int(row["rows"]) for row in source_rows)
            output_path: str | None = None
            arrow_rows: int | None = None
            polars_rows: int | None = None
            duckdb_rows: int | None = None
            error: str | None = None
            status = "passed"
            try:
                signature = _source_signature(
                    source_rows,
                    endpoint=endpoint,
                    show_progress=show_progress,
                )
                recorded = manifest.execute(
                    "SELECT source_signature,source_files,source_rows,output_path,"
                    "output_rows,status FROM compactions WHERE endpoint=?",
                    (endpoint,),
                ).fetchone()
                if recorded is None:
                    raise RuntimeError("missing compaction manifest record")
                if str(recorded["status"]) != "success":
                    raise RuntimeError(
                        f"compaction manifest status={recorded['status']!r}"
                    )
                if str(recorded["source_signature"]) != signature:
                    raise RuntimeError("source signature differs from compact output")
                if int(recorded["source_files"]) != source_files:
                    raise RuntimeError(
                        "source-file mismatch: "
                        f"current={source_files} recorded={recorded['source_files']}"
                    )
                if int(recorded["source_rows"]) != expected_rows:
                    raise RuntimeError(
                        "source-row mismatch: "
                        f"current={expected_rows} recorded={recorded['source_rows']}"
                    )
                output_path = str(recorded["output_path"] or "")
                compact_path = Path(output_path)
                arrow_rows = int(pq.ParquetFile(compact_path).metadata.num_rows)
                polars_rows, _, _, _ = _polars_validate(compact_path)
                if not (
                    arrow_rows
                    == polars_rows
                    == expected_rows
                    == int(recorded["output_rows"])
                ):
                    raise RuntimeError(
                        "row-count mismatch: "
                        f"manifest={expected_rows} recorded={recorded['output_rows']} "
                        f"pyarrow={arrow_rows} polars={polars_rows}"
                    )
                if database is None:
                    raise RuntimeError("openbb.duckdb is missing")
                view_name = _duckdb_view_name(endpoint)
                duckdb_rows = int(
                    database.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                )
                if duckdb_rows != expected_rows:
                    raise RuntimeError(
                        "DuckDB view row-count mismatch: "
                        f"manifest={expected_rows} duckdb={duckdb_rows}"
                    )
            except Exception as exc:
                status = "failed"
                error = f"{type(exc).__name__}: {str(exc)[:4000]}"
            audit_rows.append(
                {
                    "endpoint": endpoint,
                    "status": status,
                    "source_files": source_files,
                    "source_rows": expected_rows,
                    "output_path": output_path,
                    "arrow_rows": arrow_rows,
                    "polars_rows": polars_rows,
                    "duckdb_rows": duckdb_rows,
                    "error": error,
                    "checked_at": checked_at,
                }
            )
            progress.set_postfix(
                passed=sum(row["status"] == "passed" for row in audit_rows),
                failed=sum(row["status"] == "failed" for row in audit_rows),
                refresh=False,
            )
    finally:
        progress.close()
        if database is not None:
            database.close()
    return audit_rows


def _record_compaction(
    connection: sqlite3.Connection,
    result: CompactResult,
    source_signature: str,
) -> None:
    connection.execute(
        """
        INSERT INTO compactions (
            endpoint, source_signature, source_files, source_rows, output_path,
            output_rows, min_date, max_date, status, error, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(endpoint) DO UPDATE SET
            source_signature=excluded.source_signature,
            source_files=excluded.source_files,
            source_rows=excluded.source_rows,
            output_path=excluded.output_path,
            output_rows=excluded.output_rows,
            min_date=excluded.min_date,
            max_date=excluded.max_date,
            status=excluded.status,
            error=excluded.error,
            updated_at=excluded.updated_at
        """,
        (
            result.endpoint,
            source_signature,
            result.source_files,
            result.source_rows,
            result.output_path,
            result.output_rows,
            result.min_date,
            result.max_date,
            result.status,
            result.error,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    connection.commit()


def _write_summary(results: Sequence[CompactResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        [
            {
                "endpoint": item.endpoint,
                "status": item.status,
                "source_files": item.source_files,
                "source_rows": item.source_rows,
                "output_rows": item.output_rows,
                "output_path": item.output_path,
                "min_date": item.min_date,
                "max_date": item.max_date,
                "error": item.error,
            }
            for item in results
        ],
        infer_schema_length=None,
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.write_parquet(
            temporary, compression="zstd", compression_level=6, statistics=True
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir: Path = args.output_dir
    state_path = output_dir / "_state" / "openbb_archive.sqlite3"
    manifest = _open_manifest(state_path)
    grouped = _source_rows(manifest, args.endpoint, show_progress=not args.no_progress)
    if not grouped:
        print("[openbb-compact] no successful source parquet files matched", flush=True)
        manifest.close()
        return 0

    temp_dir = output_dir / "_state" / "duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    database_path = output_dir / "openbb.duckdb"
    if args.audit_only:
        audit_rows = _audit_compactions(
            manifest,
            grouped,
            database_path,
            show_progress=not args.no_progress,
        )
        manifest.close()
        _write_compaction_audit(
            audit_rows, output_dir / "catalog" / "compaction_audit.parquet"
        )
        failed = sum(row["status"] == "failed" for row in audit_rows)
        print(
            "[openbb-compact-audit] "
            f"endpoints={len(audit_rows)} passed={len(audit_rows) - failed} "
            f"failed={failed}",
            flush=True,
        )
        return 0 if failed == 0 else 2

    database = duckdb.connect(str(database_path))
    database.execute(f"SET threads={max(1, int(args.threads))}")
    database.execute(f"SET memory_limit={_sql_string(str(args.memory_limit))}")
    database.execute(f"SET temp_directory={_sql_string(str(temp_dir.resolve()))}")
    if args.no_progress:
        database.execute("PRAGMA disable_progress_bar")
    else:
        database.execute("PRAGMA enable_progress_bar")
        database.execute("SET progress_bar_time=1000")

    results: list[CompactResult] = []
    progress = tqdm(
        grouped.items(),
        desc="openbb:compact",
        unit="endpoint",
        disable=args.no_progress,
    )
    stage_progress = None
    try:
        for endpoint, rows in progress:
            stage_progress = tqdm(
                total=5,
                desc=f"compact:{endpoint}"[:64],
                unit="stage",
                position=1,
                leave=False,
                disable=args.no_progress,
            )
            signature = _source_signature(
                rows,
                endpoint=endpoint,
                show_progress=not args.no_progress,
            )
            stage_progress.update(1)
            stage_progress.set_postfix(stage="source signature", refresh=False)
            source_files = [str(row["output_path"]) for row in rows]
            source_row_count = sum(int(row["rows"]) for row in rows)
            output_path = (
                output_dir / "compact" / Path(*endpoint.split(".")) / "archive.parquet"
            )
            existing = manifest.execute(
                "SELECT source_signature, status, output_path FROM compactions WHERE endpoint=?",
                (endpoint,),
            ).fetchone()
            if (
                not args.force
                and existing is not None
                and existing["source_signature"] == signature
                and existing["status"] == "success"
                and Path(str(existing["output_path"])).is_file()
            ):
                try:
                    stage_progress.set_postfix(
                        stage="pyarrow/polars validate", refresh=False
                    )
                    output_rows, min_date, max_date = _validate_compacted_output(
                        Path(str(existing["output_path"])), source_row_count
                    )
                except Exception:
                    # A compact file is a rebuildable derivative. If it is
                    # missing rows or unreadable, regenerate it atomically
                    # from already-audited source shards instead of accepting
                    # a stale source signature as sufficient proof.
                    pass
                else:
                    stage_progress.update(3)
                    view_name = _duckdb_view_name(endpoint)
                    existing_output = Path(str(existing["output_path"])).resolve()
                    database.execute(
                        f"CREATE OR REPLACE VIEW {view_name} AS "
                        f"SELECT * FROM read_parquet({_sql_string(str(existing_output))})"
                    )
                    stage_progress.update(1)
                    stage_progress.close()
                    results.append(
                        CompactResult(
                            endpoint,
                            "skipped_unchanged",
                            len(source_files),
                            source_row_count,
                            output_rows,
                            str(existing["output_path"]),
                            min_date,
                            max_date,
                        )
                    )
                    continue

            output_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
            try:
                stage_progress.set_postfix(stage="duckdb compact", refresh=False)
                relation = database.read_parquet(source_files, union_by_name=True)
                relation.write_parquet(
                    str(temporary),
                    compression="zstd",
                    overwrite=True,
                    row_group_size=122_880,
                )
                stage_progress.update(1)
                stage_progress.set_postfix(stage="pyarrow metadata", refresh=False)
                stage_progress.update(1)
                stage_progress.set_postfix(stage="polars validate", refresh=False)
                polars_rows, min_date, max_date = _validate_compacted_output(
                    temporary, source_row_count
                )
                stage_progress.update(1)
                temporary.replace(output_path)
                view_name = _duckdb_view_name(endpoint)
                database.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f"SELECT * FROM read_parquet({_sql_string(str(output_path.resolve()))})"
                )
                stage_progress.update(1)
                result = CompactResult(
                    endpoint,
                    "success",
                    len(source_files),
                    source_row_count,
                    polars_rows,
                    str(output_path),
                    min_date,
                    max_date,
                )
            except Exception as exc:
                result = CompactResult(
                    endpoint,
                    "failed",
                    len(source_files),
                    source_row_count,
                    0,
                    None,
                    error=f"{type(exc).__name__}: {str(exc)[:4000]}",
                )
            finally:
                temporary.unlink(missing_ok=True)
                stage_progress.close()
            _record_compaction(manifest, result, signature)
            results.append(result)
            progress.set_postfix(
                success=sum(item.status == "success" for item in results),
                failed=sum(item.status == "failed" for item in results),
                refresh=False,
            )
    finally:
        if stage_progress is not None:
            stage_progress.close()
        progress.close()
        database.close()
        manifest.close()

    _write_summary(results, output_dir / "catalog" / "compaction_summary.parquet")
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    print(
        f"[openbb-compact] endpoints={len(results)} status={counts} database={database_path}",
        flush=True,
    )
    return 0 if counts.get("failed", 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(run())
