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
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import time
from typing import Iterator, Mapping, Sequence
from uuid import uuid4

import duckdb
import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm

from downloader.artifact_io import atomic_write_json

from stockagent.data.columnar_lake import (
    CompactParquetReceipt,
    SourceFileContract,
    SourceMetadataProof,
    compact_parquet_files,
    observe_source_metadata,
    parquet_schema_fingerprint,
    source_signature,
    verify_source_metadata_proofs,
)
from scripts.openbb_l1_source_journal import (
    SourceAuditPlan,
    TRACKED_TASK_COLUMNS,
    finish_source_audit,
    prepare_source_audit,
)
from scripts.openbb_l1_view_schema import (
    SCHEMA_IDENTITY_REVISION,
    schema_identities,
)


SCHEMA_VERSION = 1
SEGMENT_TABLE = "l1_compaction_segments"
MEMBER_TABLE = "l1_compaction_members"
FAILURE_TABLE = "l1_compaction_failures"
TASK_COMPACTION_INDEX = "idx_l1_tasks_compaction_order"
MAX_QUERY_VIEW_SCHEMA_VARIANTS = 4_096
STALE_SOURCE_SCAN_ORDER = "member_first"
SOURCE_AUDIT_CONTRACT_REVISION = 1
SCHEMA_GROUPED_ENDPOINT = "regulators.sec.filing_headers"
SCHEMA_GROUPED_POLICY_REVISION = 1


class _CompactorLockBusy(RuntimeError):
    """A different writer owns the manifest and its latest attempt receipt."""


class _AttemptReceiptError(RuntimeError):
    """Attempt telemetry could not be made durable."""


class _CompactionAttempt:
    """Bounded invocation evidence, separate from the published data contract.

    Only the compactor lock owner writes latest. A losing writer emits its own
    terminal journal event instead, so it cannot replace the owner's evidence.
    An uncatchable kill leaves running evidence, never a fabricated completion.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.started = time.monotonic()
        self.phase_started = self.started
        self.finalized = False
        self.path = args.output_dir.resolve() / "_state/l1_compaction_attempt_latest.json"
        invocation = os.environ.get("INVOCATION_ID", "")
        self.payload = {
            "schema_version": 1,
            "event": "openbb_l1_compaction_attempt",
            "attempt_id": uuid4().hex,
            "systemd_invocation_id": (
                invocation if re.fullmatch(r"[0-9a-f]{32}", invocation) else None
            ),
            "started_at_utc": _now(),
            "finished_at_utc": None,
            "elapsed_seconds": 0.0,
            "state": "running",
            "reason": None,
            "exit_code": None,
            "exit_code_basis": "process_exit",
            "mode": "audit" if args.audit_only else "compact",
            "publication_stage": "not_attempted",
            "phase": "lock",
            "phase_elapsed_seconds": 0.0,
            "new_segments": 0,
            "stale_segments": 0,
            "failed_segments": 0,
            "deferred_failed_segments": 0,
            "stage_seconds": {},
            "stage_resources": {"before_attempt": _stage_resources()},
        }

    def phase(self, name: str) -> None:
        self.payload["phase"] = name
        self.phase_started = time.monotonic()

    def fail(self, exc: BaseException) -> None:
        interrupted = isinstance(exc, (KeyboardInterrupt, SystemExit))
        code = 1
        if isinstance(exc, SystemExit):
            # Match the actual POSIX process result, including SystemExit(None).
            # A zero exit still has interrupted state, never business completed.
            code = (int(exc.code) % 256 if isinstance(exc.code, int)
                    else 0 if exc.code is None else 1)
        elif isinstance(exc, KeyboardInterrupt):
            code = 130
            self.payload.update(exit_code_basis="shell_signal", signal_number=2)
        self.payload.update(
            state="interrupted" if interrupted else "failed",
            reason=(
                "interrupted" if interrupted else
                "compactor_lock_busy" if isinstance(exc, _CompactorLockBusy) else
                "attempt_receipt_error" if isinstance(exc, _AttemptReceiptError) else
                "exception"
            ),
            exit_code=code,
            exception_type=type(exc).__name__[:128],
        )

    def finish(self, *, persist: bool) -> None:
        self.payload.update(
            finished_at_utc=_now(),
            elapsed_seconds=round(time.monotonic() - self.started, 6),
            phase_elapsed_seconds=round(time.monotonic() - self.phase_started, 6),
        )
        self.payload["stage_resources"]["after_attempt"] = _stage_resources()
        write_error = None
        if persist:
            try:
                atomic_write_json(self.path, self.payload)
            except Exception as exc:
                write_error = exc
                self.payload["receipt_write_failed"] = True
                if self.payload["state"] not in {"failed", "interrupted"}:
                    self.fail(_AttemptReceiptError())
        self.finalized = True
        # Fixed scalar fields and stage dictionaries only: never endpoint lists,
        # provider responses, exception messages, credentials, or raw source data.
        try:
            print(json.dumps(self.payload, separators=(",", ":")), flush=True)
        except OSError:
            pass  # A broken output stream must not hide the original exception.
        if write_error is not None:
            raise _AttemptReceiptError("cannot persist compaction attempt") from write_error

    @contextmanager
    def record(self) -> Iterator[None]:
        """Called strictly inside the exclusive compactor lock."""
        try:
            try:
                atomic_write_json(self.path, self.payload)
            except Exception as exc:
                raise _AttemptReceiptError("cannot persist compaction start") from exc
            yield
        except BaseException as exc:
            try:
                self.fail(exc)
                self.finish(persist=True)
            except Exception:
                pass  # Preserve the business error if telemetry also fails.
            raise
        else:
            self.finish(persist=True)


@dataclass(frozen=True, slots=True)
class ViewSourceProof:
    segment_id: str
    path: str
    file_signature: tuple[int, int, int, int, int]
    output_rows: int
    arrow_fingerprint: str
    physical_identity: str | None


def _view_file_signature(path: Path) -> tuple[int, int, int, int, int]:
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f"L1 view source is not a regular file: {path}")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _view_source_proof(
    path: Path, segment_id: str, parquet_file: pq.ParquetFile,
    before: tuple[int, int, int, int, int], *, output_rows: int,
    output_bytes: int, arrow_fingerprint: str,
) -> ViewSourceProof:
    observed_arrow, identity = schema_identities(parquet_file)
    if (
        parquet_file.metadata.num_rows != output_rows
        or before[2] != output_bytes
        or observed_arrow != arrow_fingerprint
    ):
        raise RuntimeError(f"L1 view source receipt mismatch: {path}")
    if _view_file_signature(path) != before:
        raise RuntimeError(f"L1 view source changed during metadata audit: {path}")
    return ViewSourceProof(
        segment_id, _lexical_absolute_path(path), before, output_rows, observed_arrow, identity
    )


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
    schema_fingerprint: str = ""
    metadata_proof: SourceMetadataProof | None = None

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
        "--archive-idle-only",
        action="store_true",
        help=(
            "Run only while the live archive downloader is waiting for quota; "
            "stop between segments when downloading resumes."
        ),
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Deep-audit active L1 segments, source contracts, and query views.",
    )
    parser.add_argument(
        "--incremental-source-audit",
        action="store_true",
        help=(
            "After a full baseline, audit task changes from same-transaction "
            "SQLite triggers; periodic/schema-drift fallbacks stay full."
        ),
    )
    parser.add_argument(
        "--force-full-source-audit",
        action="store_true",
        help="Force the full source comparison and refresh the journal baseline.",
    )
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument(
        "--schema-grouped-sec-views", action="store_true",
        help=("Opt in to receipt- and physical-schema-proven SEC filing_headers "
              "view groups; all other endpoints keep union_by_name=true."),
    )
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
    if args.force_full_source_audit and not args.incremental_source_audit:
        parser.error("--force-full-source-audit requires --incremental-source-audit")
    if args.incremental_source_audit and args.endpoint:
        parser.error(
            "--incremental-source-audit requires the full endpoint universe; "
            "filtered audits cannot advance a global task-change watermark"
        )
    return args


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def _stage_resources() -> dict[str, int | None]:
    """Observe cumulative CPU/wall, memory and stall counters without altering work."""

    snapshot: dict[str, int | None] = {
        # Cumulative monotonic counters, not extra wall-time stages to sum.
        # Difference adjacent samples to distinguish CPU work from waiting.
        "process_cpu_ns": time.process_time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "process_rss_bytes": None,
        "process_swap_bytes": None,
        "process_read_bytes": None,
        "process_write_bytes": None,
        "cgroup_memory_bytes": None,
        "cgroup_memory_peak_bytes": None,
        "cgroup_memory_anon_bytes": None,
        "cgroup_memory_file_bytes": None,
        "cgroup_swap_bytes": None,
        "cgroup_memory_high_events": None,
        "cgroup_memory_max_events": None,
        "cgroup_memory_oom_events": None,
        "cgroup_memory_oom_kill_events": None,
        "cgroup_memory_full_stall_us": None,
        "cgroup_io_full_stall_us": None,
    }
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition(":")
            if not separator or key not in {"VmRSS", "VmSwap"}:
                continue
            amount = int(value.strip().split()[0]) * 1024
            snapshot["process_rss_bytes" if key == "VmRSS" else "process_swap_bytes"] = amount
        for line in Path("/proc/self/io").read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition(":")
            if separator and key in {"read_bytes", "write_bytes"}:
                snapshot[f"process_{key}"] = int(value.strip())
        for line in Path("/proc/self/cgroup").read_text(encoding="utf-8").splitlines():
            if not line.startswith("0::"):
                continue
            relative = line.removeprefix("0::").lstrip("/")
            group = Path("/sys/fs/cgroup") / relative
            for field, filename in (
                ("cgroup_memory_bytes", "memory.current"),
                ("cgroup_memory_peak_bytes", "memory.peak"),
                ("cgroup_swap_bytes", "memory.swap.current"),
            ):
                path = group / filename
                if path.is_file():
                    try:
                        snapshot[field] = int(path.read_text(encoding="utf-8").strip())
                    except (OSError, ValueError):
                        pass
            statistics = group / "memory.stat"
            if statistics.is_file():
                for statistic in statistics.read_text(encoding="utf-8").splitlines():
                    name, _, value = statistic.partition(" ")
                    if name in {"anon", "file"}:
                        try:
                            snapshot[f"cgroup_memory_{name}_bytes"] = int(value)
                        except ValueError:
                            pass
            events = group / "memory.events"
            if events.is_file():
                for event in events.read_text(encoding="utf-8").splitlines():
                    name, _, value = event.partition(" ")
                    if name in {"high", "max", "oom", "oom_kill"}:
                        try:
                            snapshot[f"cgroup_memory_{name}_events"] = int(value)
                        except ValueError:
                            pass
            for field, filename in (
                ("cgroup_memory_full_stall_us", "memory.pressure"),
                ("cgroup_io_full_stall_us", "io.pressure"),
            ):
                path = group / filename
                if path.is_file():
                    for pressure in path.read_text(encoding="utf-8").splitlines():
                        if pressure.startswith("full "):
                            snapshot[field] = int(
                                dict(item.split("=", 1) for item in pressure.split()[1:])["total"]
                            )
                            break
            break
    except (OSError, ValueError, IndexError):
        pass
    return snapshot


def _lexical_absolute_path(value: object) -> str:
    """Normalize a manifest path without issuing filesystem lookups.

    This function runs inside a SQLite join for every compacted source member.
    ``Path.resolve()`` turned that set comparison into millions of unnecessary
    filesystem calls. A lexical absolute path is sufficient here: members are
    recorded from canonical absolute task paths, and a later symlink spelling
    change is conservatively treated as a changed source contract.
    """

    return os.path.abspath(os.path.normpath(str(value)))


def _live_pid(path: Path) -> int | None:
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
    except (OSError, ValueError):
        return None
    return pid


def _archive_compaction_allowed(state_dir: Path) -> tuple[bool, str]:
    """Return whether derivative work can run without stealing archive I/O."""

    downloader_pid = _live_pid(state_dir / "downloader.pid")
    if downloader_pid is None:
        return True, "archive_downloader_not_running"

    phase_path = state_dir / "downloader_phase.json"
    phase = _read_json_object(phase_path)
    phase_name = str(phase.get("phase") or "unknown")
    if phase_name != "download":
        return False, f"archive_phase_{phase_name}"

    scheduler_path = state_dir / "provider_scheduler.json"
    scheduler = _read_json_object(scheduler_path)
    try:
        scheduler_is_current = (
            scheduler_path.stat().st_mtime_ns >= phase_path.stat().st_mtime_ns
        )
    except OSError:
        scheduler_is_current = False
    if not scheduler_is_current:
        return False, "archive_scheduler_not_current"
    if str(scheduler.get("phase") or "unknown") != "waiting":
        return False, f"archive_scheduler_{scheduler.get('phase') or 'unknown'}"
    for key in ("active_total", "buffered_total", "completed_pending_total"):
        if int(scheduler.get(key) or 0) > 0:
            return False, f"archive_scheduler_{key}"
    return True, "archive_waiting_for_provider_quota"


def _clean_duckdb_temp_files(path: Path) -> int:
    """Remove spill files left by an interrupted compactor under its lock."""

    if not path.is_dir():
        return 0
    removed = 0
    for item in path.iterdir():
        if item.is_file():
            item.unlink(missing_ok=True)
            removed += 1
    return removed


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
            raise _CompactorLockBusy(f"another OpenBB L1 compactor owns {path}") from exc
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
        _lexical_absolute_path,
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
        CREATE TABLE IF NOT EXISTS {FAILURE_TABLE} (
            source_signature TEXT PRIMARY KEY,
            endpoint TEXT NOT NULL,
            source_files INTEGER NOT NULL,
            attempts INTEGER NOT NULL,
            last_error TEXT NOT NULL,
            next_retry_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_l1_segments_endpoint_status
            ON {SEGMENT_TABLE}(endpoint, status);
        CREATE INDEX IF NOT EXISTS idx_l1_members_segment
            ON {MEMBER_TABLE}(segment_id);
        CREATE INDEX IF NOT EXISTS idx_l1_members_endpoint
            ON {MEMBER_TABLE}(endpoint);
        CREATE INDEX IF NOT EXISTS idx_l1_failures_retry
            ON {FAILURE_TABLE}(next_retry_at, endpoint);
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


def _stale_source_contract_sql(
    filter_clause: str, *, member_first: bool = False
) -> str:
    source_join = (
        f"FROM {MEMBER_TABLE} AS m "
        f"CROSS JOIN {SEGMENT_TABLE} AS s ON s.segment_id=m.segment_id"
        if member_first
        else f"FROM {SEGMENT_TABLE} AS s "
             f"JOIN {MEMBER_TABLE} AS m ON m.segment_id=s.segment_id"
    )
    return f"""
        SELECT DISTINCT s.segment_id, s.endpoint, s.output_path,
            CASE
                WHEN t.task_id IS NULL THEN 'source task missing from manifest'
                WHEN t.active!=1 THEN 'source task retired from active plan'
                WHEN t.status!='success' THEN 'source task is no longer successful'
                WHEN t.endpoint!=m.endpoint OR t.endpoint!=s.endpoint
                    THEN 'source endpoint changed'
                WHEN (? || t.output_path)!=m.source_path
                    AND stockagent_resolve_path(t.output_path)!=m.source_path
                    THEN 'source path changed'
                WHEN t.rows!=m.source_rows THEN 'source row contract changed'
                WHEN t.updated_at!=m.task_updated_at THEN 'source task was refreshed'
                ELSE NULL
            END AS stale_reason
        {source_join}
        LEFT JOIN tasks AS t ON t.task_id=m.task_id
        WHERE s.status='success'{filter_clause}
          AND (
              t.task_id IS NULL OR t.active!=1 OR t.status!='success'
              OR t.endpoint!=m.endpoint OR t.endpoint!=s.endpoint
              OR ((? || t.output_path)!=m.source_path
                  AND stockagent_resolve_path(t.output_path)!=m.source_path)
              OR t.rows!=m.source_rows
              OR t.updated_at!=m.task_updated_at
          )
        ORDER BY s.segment_id
        """


def _source_audit_fingerprint() -> str:
    source_sql = _stale_source_contract_sql("", member_first=True)
    referenced_columns = set(re.findall(r"\bt\.([a-z_]+)\b", source_sql))
    if not referenced_columns <= TRACKED_TASK_COLUMNS:
        raise RuntimeError(
            "OpenBB L1 stale source SQL uses untracked task columns: "
            + ", ".join(sorted(referenced_columns - TRACKED_TASK_COLUMNS))
        )
    normalizer = _lexical_absolute_path.__code__
    return hashlib.sha256(
        source_sql.encode()
        + normalizer.co_code
        + repr(normalizer.co_consts).encode()
        + str(SOURCE_AUDIT_CONTRACT_REVISION).encode()
    ).hexdigest()


def _mark_stale_segments(
    connection: sqlite3.Connection,
    filters: Sequence[str],
    *,
    changed_task_ids: Sequence[str] | None = None,
    timing: dict[str, float] | None = None,
    view_proofs: dict[str, ViewSourceProof] | None = None,
) -> tuple[set[str], int]:
    stage_started = time.monotonic()
    # Source members store absolute paths while the archive normally records
    # cwd-relative paths.  Compare that common, exact spelling inside SQLite
    # before calling the Python normalizer millions of times.  The fallback is
    # required for absolute, dotted and otherwise noncanonical source paths.
    source_prefix = os.path.abspath(os.curdir) + os.sep
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
    if timing is not None:
        timing["stale_existing_status_query"] = round(
            time.monotonic() - stage_started, 3
        )
    source_query_started = time.monotonic()
    rows: list[sqlite3.Row] = []
    if changed_task_ids is None:
        # The full comparison remains the bootstrap, daily and fail-closed
        # fallback path. No journal state can waive this source contract.
        rows = connection.execute(
            _stale_source_contract_sql(
                filter_clause,
                member_first=STALE_SOURCE_SCAN_ORDER == "member_first",
            ),
            (source_prefix, *filter_parameters, source_prefix),
        ).fetchall()
    else:
        # The journal supplies exact mutated task IDs. Bounded IN batches use
        # the member task primary key; new un-compacted tasks simply match no
        # member. Keep the same stale predicates and derivative metadata audit.
        for start in range(0, len(changed_task_ids), 400):
            batch = changed_task_ids[start:start + 400]
            narrowed = (
                filter_clause + " AND m.task_id IN ("
                + ",".join("?" for _ in batch) + ")"
            )
            rows.extend(connection.execute(
                _stale_source_contract_sql(narrowed, member_first=True),
                (source_prefix, *filter_parameters, *batch, source_prefix),
            ).fetchall())
    if timing is not None:
        timing["stale_source_join_query"] = round(
            time.monotonic() - source_query_started, 3
        )
        timing["stale_source_contract_query"] = round(
            time.monotonic() - stage_started, 3
        )
    # Missing derivatives are stale even when the source task contract did not
    # change. Checking one path per segment is bounded by the segment count.
    stage_started = time.monotonic()
    known = {str(row["segment_id"]): row for row in rows}
    for row in connection.execute(
        f"""
        SELECT segment_id, endpoint, output_path, output_rows, output_bytes,
               schema_fingerprint
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
            # Opening the Parquet metadata already checks path existence. Avoid
            # an extra stat for every healthy segment; classify absent or
            # non-file paths only on the exceptional path below.
            collect_proof = (
                view_proofs is not None and row["endpoint"] == SCHEMA_GROUPED_ENDPOINT
            )
            before = _view_file_signature(path) if collect_proof else None
            parquet_file = pq.ParquetFile(str(path))
            if before is not None:
                # The proof validates rows, bytes and the manifest Arrow hash
                # from this same open footer. Do not serialize its Arrow schema
                # again in the ordinary audit or physical identity builder.
                proof = _view_source_proof(
                    path, str(row["segment_id"]), parquet_file, before,
                    output_rows=int(row["output_rows"]),
                    output_bytes=int(row["output_bytes"]),
                    arrow_fingerprint=str(row["schema_fingerprint"]),
                )
                view_proofs[proof.path] = proof
            elif int(parquet_file.metadata.num_rows) != int(row["output_rows"]):
                derivative_error = "L1 output row count changed"
            elif (
                hashlib.sha256(
                    parquet_file.schema_arrow.remove_metadata()
                    .serialize()
                    .to_pybytes()
                ).hexdigest()
                != row["schema_fingerprint"]
            ):
                derivative_error = "L1 output schema fingerprint changed"
        except MemoryError:
            # Resource exhaustion is not evidence of corrupt/missing data.
            # Abort before stale-state mutations or derivative quarantine.
            raise
        except Exception as exc:
            derivative_error = (
                "L1 output file is missing"
                if not path.is_file()
                else f"L1 output is unreadable: {type(exc).__name__}: {exc}"
            )
        if derivative_error is not None:
            known[str(row["segment_id"])] = {
                "segment_id": row["segment_id"],
                "endpoint": row["endpoint"],
                "output_path": row["output_path"],
                "stale_reason": derivative_error,
            }

    if timing is not None:
        timing["stale_derivative_metadata_scan"] = round(
            time.monotonic() - stage_started, 3
        )

    stage_started = time.monotonic()
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
    if timing is not None:
        timing["stale_state_apply"] = round(time.monotonic() - stage_started, 3)
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
    timing: dict[str, float] | None = None,
) -> list[TaskShard]:
    plan_token = _active_plan_token(connection)
    token_clause = "" if plan_token is None else " AND t.plan_token=?"
    parameters: list[object] = [] if plan_token is None else [plan_token]
    filter_clause, filter_parameters = _filter_sql(filters, alias="t")
    parameters.extend(filter_parameters)
    parameters.append(int(limit))
    query_started = time.monotonic()
    global_sql = f"""
        SELECT t.task_id, t.endpoint, t.output_path, t.rows, t.updated_at
        FROM tasks AS t
        LEFT JOIN {MEMBER_TABLE} AS m ON m.task_id=t.task_id
        WHERE t.active=1 AND t.status='success' AND m.task_id IS NULL
          {token_clause}{filter_clause}
        ORDER BY t.endpoint, t.task_id
        LIMIT ?
        """
    rows: list[sqlite3.Row] | None = None
    # The active-plan index yields endpoints in order, but not task IDs.  When
    # the first unassigned endpoint fills this batch, sorting just that
    # endpoint is identical to the global ORDER BY and avoids sorting every
    # other endpoint's successful task.  Keep the original query for a short
    # first endpoint, explicit endpoint filters, or manifests without the
    # downloader's active-plan index.
    task_indexes = {
        str(row[1]) for row in connection.execute("PRAGMA index_list(tasks)")
    }
    active_plan_index = "idx_tasks_active_plan" in task_indexes
    if (
        plan_token is not None
        and not filters
        and 0 < limit <= 8192
        and TASK_COMPACTION_INDEX in task_indexes
    ):
        # The partial covering index preserves the original global
        # endpoint/task-id order without sorting millions of unassigned
        # successes. Only the selected batch needs table-row lookups.
        rows = connection.execute(
            f"""
            SELECT t.task_id, t.endpoint, t.output_path, t.rows, t.updated_at
            FROM tasks AS t INDEXED BY {TASK_COMPACTION_INDEX}
            LEFT JOIN {MEMBER_TABLE} AS m ON m.task_id=t.task_id
            WHERE t.active=1 AND t.plan_token=? AND t.status='success'
              AND m.task_id IS NULL
            ORDER BY t.endpoint, t.task_id LIMIT ?
            """,
            (plan_token, int(limit)),
        ).fetchall()
        if timing is not None:
            timing["unassigned_source_index_order"] = round(
                time.monotonic() - query_started, 3
            )
    elif plan_token is not None and not filters and 0 < limit <= 8192 and active_plan_index:
        connection.execute("SAVEPOINT l1_unassigned_read")
        try:
            endpoint_started = time.monotonic()
            first = connection.execute(
                f"""
                SELECT t.endpoint
                FROM tasks AS t INDEXED BY idx_tasks_active_plan
                WHERE t.active=1 AND t.plan_token=? AND t.status='success'
                  AND NOT EXISTS (
                      SELECT 1 FROM {MEMBER_TABLE} AS m WHERE m.task_id=t.task_id
                  )
                ORDER BY t.endpoint LIMIT 1
                """,
                (plan_token,),
            ).fetchone()
            if timing is not None:
                timing["unassigned_source_first_endpoint"] = round(
                    time.monotonic() - endpoint_started, 3
                )
            if first is None:
                rows = []
            else:
                endpoint_started = time.monotonic()
                candidate = connection.execute(
                    f"""
                    SELECT t.task_id, t.endpoint, t.output_path, t.rows,
                           t.updated_at
                    FROM tasks AS t INDEXED BY idx_tasks_active_plan
                    LEFT JOIN {MEMBER_TABLE} AS m ON m.task_id=t.task_id
                    WHERE t.active=1 AND t.plan_token=? AND t.status='success'
                      AND t.endpoint=? AND m.task_id IS NULL
                    ORDER BY t.task_id LIMIT ?
                    """,
                    (plan_token, str(first[0]), int(limit)),
                ).fetchall()
                if timing is not None:
                    timing["unassigned_source_endpoint_sort"] = round(
                        time.monotonic() - endpoint_started, 3
                    )
                if len(candidate) == limit:
                    rows = candidate
            if rows is None:
                fallback_started = time.monotonic()
                rows = connection.execute(global_sql, parameters).fetchall()
                if timing is not None:
                    timing["unassigned_source_global_fallback"] = round(
                        time.monotonic() - fallback_started, 3
                    )
        except BaseException:
            connection.execute("ROLLBACK TO l1_unassigned_read")
            raise
        finally:
            connection.execute("RELEASE l1_unassigned_read")
    else:
        fallback_started = time.monotonic()
        rows = connection.execute(global_sql, parameters).fetchall()
        if timing is not None:
            timing["unassigned_source_global_fallback"] = round(
                time.monotonic() - fallback_started, 3
            )
    if timing is not None:
        timing["unassigned_source_query"] = round(
            time.monotonic() - query_started, 3
        )
    metadata_started = time.monotonic()
    output: list[TaskShard] = []
    progress = tqdm(
        rows,
        desc="openbb:l1 source contracts",
        unit="file",
        disable=not show_progress,
    )
    for row in progress:
        proof = observe_source_metadata(str(row["output_path"]))
        path = Path(proof.path)
        metadata_rows = proof.rows
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
                bytes=proof.bytes,
                uncompressed_bytes=proof.uncompressed_bytes,
                mtime_ns=proof.file_signature[3],
                schema_fingerprint=proof.schema_fingerprint,
                metadata_proof=proof,
            )
        )
    if timing is not None:
        timing["unassigned_source_metadata"] = round(
            time.monotonic() - metadata_started, 3
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
    by_endpoint_schema: dict[tuple[str, str], list[TaskShard]] = defaultdict(list)
    for shard in shards:
        by_endpoint_schema[(shard.endpoint, shard.schema_fingerprint)].append(shard)
    for endpoint, schema_fingerprint in sorted(by_endpoint_schema):
        batch: list[TaskShard] = []
        bytes_in_batch = 0
        uncompressed_bytes_in_batch = 0
        rows_in_batch = 0
        for shard in by_endpoint_schema[(endpoint, schema_fingerprint)]:
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
        if batch and (len(batch) >= min_files or include_tail or endpoint in forced):
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


def _failure_retry_ready(
    connection: sqlite3.Connection, signature: str
) -> tuple[bool, str | None]:
    row = connection.execute(
        f"SELECT next_retry_at FROM {FAILURE_TABLE} WHERE source_signature=?",
        (signature,),
    ).fetchone()
    if row is None:
        return True, None
    retry_at = str(row["next_retry_at"])
    try:
        return datetime.fromisoformat(retry_at) <= datetime.now(timezone.utc), retry_at
    except ValueError:
        return True, retry_at


def _record_compaction_failure(
    connection: sqlite3.Connection,
    *,
    signature: str,
    endpoint: str,
    source_files: int,
    error: Exception,
) -> str:
    row = connection.execute(
        f"SELECT attempts FROM {FAILURE_TABLE} WHERE source_signature=?",
        (signature,),
    ).fetchone()
    attempts = int(row["attempts"] or 0) + 1 if row is not None else 1
    delay_seconds = min(24 * 60 * 60, 60 * 60 * (2 ** min(4, attempts - 1)))
    retry_at = (
        datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    ).isoformat()
    with connection:
        connection.execute(
            f"""
            INSERT INTO {FAILURE_TABLE} (
                source_signature, endpoint, source_files, attempts,
                last_error, next_retry_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_signature) DO UPDATE SET
                endpoint=excluded.endpoint,
                source_files=excluded.source_files,
                attempts=excluded.attempts,
                last_error=excluded.last_error,
                next_retry_at=excluded.next_retry_at,
                updated_at=excluded.updated_at
            """,
            (
                signature,
                endpoint,
                int(source_files),
                attempts,
                f"{type(error).__name__}: {str(error)[:4000]}",
                retry_at,
                _now(),
            ),
        )
    return retry_at


def _clear_compaction_failure(connection: sqlite3.Connection, signature: str) -> None:
    with connection:
        connection.execute(
            f"DELETE FROM {FAILURE_TABLE} WHERE source_signature=?", (signature,)
        )


def _segment_output(output_dir: Path, endpoint: str, segment_id: str) -> Path:
    return (
        output_dir
        / "compact_l1"
        / Path(*endpoint.split("."))
        / "segments"
        / f"segment-{segment_id}.parquet"
    )


def _verify_source_tasks(
    connection: sqlite3.Connection, endpoint: str, shards: Sequence[TaskShard],
    *, verify_files: bool = True,
) -> None:
    for shard in shards:
        current = connection.execute(
            """
            SELECT endpoint, output_path, rows, updated_at, status, active
            FROM tasks WHERE task_id=?
            """,
            (shard.task_id,),
        ).fetchone()
        proof = shard.metadata_proof
        # Exact canonical names need no second parent-directory traversal:
        # the proof's file-identity fence follows any replacement. Preserve
        # the original resolve check for aliases and all no-proof callers.
        same_observed_path = (
            current is not None and proof is not None
            and str(current["output_path"]) == shard.output_path
        )
        if current is None or (
            current["status"] != "success"
            or int(current["active"] or 0) != 1
            or current["endpoint"] != endpoint
            or (not same_observed_path
                and str(Path(str(current["output_path"])).resolve()) != shard.output_path)
            or int(current["rows"] or -1) != shard.rows
            or current["updated_at"] != shard.task_updated_at
        ):
            raise RuntimeError(
                "source task changed while segment was being built: "
                f"task={shard.task_id}"
            )
        if proof is not None:
            if (
                proof.rows != shard.rows or proof.bytes != shard.bytes
                or proof.uncompressed_bytes != shard.uncompressed_bytes
                or proof.file_signature[3] != shard.mtime_ns
                or proof.schema_fingerprint != shard.schema_fingerprint
            ):
                raise RuntimeError(f"source metadata proof contract mismatch: {shard.task_id}")
            if verify_files:
                verify_source_metadata_proofs([shard.output_path], [proof])


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
        _verify_source_tasks(connection, endpoint, shards)
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


def _query_view_deferred_reason(schema_variants: int) -> str | None:
    if int(schema_variants) <= MAX_QUERY_VIEW_SCHEMA_VARIANTS:
        return None
    return (
        f"schema_variants={int(schema_variants)} exceeds "
        f"limit={MAX_QUERY_VIEW_SCHEMA_VARIANTS}; long_form_normalization_required"
    )


def _active_segment_paths(
    connection: sqlite3.Connection,
    *,
    included_endpoints: set[str],
    view_proofs: Mapping[str, ViewSourceProof] | None = None,
) -> dict[str, list[Path]]:
    grouped: dict[str, list[Path]] = defaultdict(list)
    for row in connection.execute(
        f"""
        SELECT endpoint, output_path, segment_id, output_rows, output_bytes,
               schema_fingerprint FROM {SEGMENT_TABLE}
        WHERE status='success' ORDER BY endpoint, segment_id
        """
    ):
        endpoint = str(row["endpoint"])
        if endpoint not in included_endpoints:
            continue
        path = Path(_lexical_absolute_path(row["output_path"]))
        proof = (view_proofs or {}).get(str(path))
        if proof is not None and (
            endpoint != SCHEMA_GROUPED_ENDPOINT
            or proof.segment_id != str(row["segment_id"])
            or proof.output_rows != int(row["output_rows"])
            or proof.file_signature[2] != int(row["output_bytes"])
            or proof.arrow_fingerprint != str(row["schema_fingerprint"])
        ):
            raise RuntimeError(f"L1 view manifest changed after metadata audit: {path}")
        grouped[endpoint].append(path)
    return dict(grouped)


def _view_input_signature(paths: Sequence[Path]) -> str:
    """Fingerprint the exact ordered path list embedded in one DuckDB view."""

    digest = hashlib.sha256()
    for path in paths:
        encoded = os.fspath(path).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, byteorder="little"))
        digest.update(encoded)
    return digest.hexdigest()


def _parquet_view_query(paths: Sequence[Path], *, grouped: bool = False) -> str:
    path_sql = "[" + ",".join(_sql_string(path) for path in paths) + "]"
    options = "union_by_name=false, hive_partitioning=false" if grouped else "union_by_name=true"
    return f"SELECT * FROM read_parquet({path_sql}, {options})"


def _schema_group_reader_policy(database: duckdb.DuckDBPyConnection) -> dict[str, object]:
    # These defaults affect Parquet logical types or UNION coercion. Pin their
    # actual values, loaded readers, and engine source ID in the reuse contract.
    return {
        "revision": SCHEMA_GROUPED_POLICY_REVISION,
        "physical_identity_revision": SCHEMA_IDENTITY_REVISION,
        "reader": database.execute("PRAGMA version").fetchall(),
        "settings": database.execute(
            "SELECT name, value FROM duckdb_settings() WHERE lower(name) IN "
            "('timezone','binary_as_string','enable_geoparquet_conversion',"
            "'old_implicit_casting') ORDER BY name"
        ).fetchall(),
        "extensions": database.execute(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE loaded ORDER BY extension_name"
        ).fetchall(),
        "options": {"union_by_name": False, "hive_partitioning": False},
        "outer_union": "ALL BY NAME",
    }


def _schema_grouped_view(
    database: duckdb.DuckDBPyConnection, paths: Sequence[Path], output_dir: Path,
    proofs: Mapping[str, ViewSourceProof],
) -> tuple[str, str, dict[str, object]] | None:
    """Prove groups from this run's footer audit; never use Arrow-only groups."""
    groups: dict[str, list[Path]] = {}
    ordered_proofs: list[ViewSourceProof] = []
    canonical_parent = _segment_output(output_dir, SCHEMA_GROUPED_ENDPOINT, "").parent
    parent_is_canonical = canonical_parent.resolve() == canonical_parent
    for path in paths:
        proof = proofs.get(str(path))
        if proof is None or proof.physical_identity is None:
            return None
        expected = _segment_output(output_dir, SCHEMA_GROUPED_ENDPOINT, proof.segment_id)
        if (
            proof.path != str(path)
            or not re.fullmatch(r"[0-9a-f]{24}(?:-r[1-9][0-9]*)?", proof.segment_id)
            or path != expected
            or not parent_is_canonical
            or path.is_symlink()
            or any("=" in part for part in path.parts)
        ):
            # Only canonical writer output is eligible. Hive partitions, aliases
            # and arbitrary external Parquet retain the original reader path.
            return None
        groups.setdefault(proof.physical_identity, []).append(path)
        ordered_proofs.append(proof)
    if not groups or len(groups) > MAX_QUERY_VIEW_SCHEMA_VARIANTS:
        return None
    representatives = [members[0] for members in groups.values()]
    query = " UNION ALL BY NAME ".join(
        f"({_parquet_view_query(members, grouped=True)})" for members in groups.values()
    )
    # Compare against DuckDB's true multi-file union, not an Arrow type guess.
    # Both DESCRIBEs are metadata-only; no row materialization is needed.
    expected_schema = database.execute(
        "DESCRIBE " + _parquet_view_query(representatives)
    ).fetchall()
    actual_schema = database.execute("DESCRIBE " + query).fetchall()
    if actual_schema != expected_schema:
        return None
    policy = _schema_group_reader_policy(database)
    signature = hashlib.sha256(json.dumps(
        {"policy": policy, "sources": [asdict(proof) for proof in ordered_proofs]},
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return query, signature, policy


def _verify_view_sources(
    signatures: Mapping[Path, tuple[int, int, int, int, int]],
) -> None:
    for path, expected in signatures.items():
        if _view_file_signature(path) != expected:
            raise RuntimeError(f"L1 view source changed after metadata audit: {path}")


def _reusable_view_catalog(
    path: Path,
    *,
    config: Mapping[str, object],
) -> dict[str, tuple[str, str, str | None, str | None]] | None:
    """Trust an old catalog only when its signature and view set are complete."""

    if not path.is_file():
        return None
    try:
        database = duckdb.connect(str(path), read_only=True, config=dict(config))
    except duckdb.OutOfMemoryException:
        raise
    except duckdb.Error:
        return None
    try:
        columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info('l1_catalog')").fetchall()
        }
        if not {"view_input_signature", "view_sql_signature"} <= columns:
            return None
        catalog: dict[str, tuple[str, str, str | None, str | None]] = {}
        expected_views: set[str] = set()
        for endpoint, view_name, state, signature, sql_signature in database.execute(
            "SELECT endpoint, view_name, query_state, view_input_signature, "
            "view_sql_signature "
            "FROM l1_catalog"
        ).fetchall():
            endpoint = str(endpoint)
            if endpoint in catalog or state not in {"published", "deferred"}:
                return None
            if state == "published":
                if (
                    view_name != _view_name(endpoint)
                    or not isinstance(signature, str)
                    or len(signature) != 64
                    or not isinstance(sql_signature, str)
                    or len(sql_signature) != 64
                ):
                    return None
                expected_views.add(view_name)
            elif view_name is not None or signature is not None or sql_signature is not None:
                return None
            catalog[endpoint] = (
                str(state), str(view_name) if view_name else "",
                signature, sql_signature,
            )
        actual_views = {
            str(name): str(sql)
            for name, sql in database.execute(
                "SELECT view_name, sql FROM duckdb_views() "
                "WHERE schema_name='main' AND view_name LIKE 'openbb_l1_%'"
            ).fetchall()
        }
        if set(actual_views) != expected_views:
            return None
        for state, view_name, _, sql_signature in catalog.values():
            if state == "published" and hashlib.sha256(
                actual_views[view_name].encode("utf-8")
            ).hexdigest() != sql_signature:
                return None
        return catalog
    except duckdb.OutOfMemoryException:
        raise
    except (duckdb.Error, RuntimeError, ValueError):
        return None
    finally:
        database.close()


def _publish_views(
    connection: sqlite3.Connection,
    database_path: Path,
    *,
    threads: int,
    memory_limit: str,
    schema_grouped_sec_views: bool = False,
    view_proofs: Mapping[str, ViewSourceProof] | None = None,
) -> tuple[int, dict[str, str], dict[str, float], dict[str, str]]:
    endpoint_statistics = {
        str(row["endpoint"]): row
        for row in connection.execute(
            f"""
            SELECT endpoint, COUNT(*) AS segment_count,
                   COUNT(DISTINCT schema_fingerprint) AS schema_variants,
                   SUM(source_files) AS source_files, SUM(output_rows) AS rows,
                   SUM(source_bytes) AS input_bytes,
                   SUM(output_bytes) AS output_bytes, MAX(updated_at) AS updated_at
            FROM {SEGMENT_TABLE}
            WHERE status='success'
            GROUP BY endpoint
            ORDER BY endpoint
            """
        )
    }
    deferred = {
        endpoint: reason
        for endpoint, row in endpoint_statistics.items()
        if (reason := _query_view_deferred_reason(int(row["schema_variants"] or 0)))
        is not None
    }
    included = set(endpoint_statistics) - set(deferred)
    grouped = _active_segment_paths(
        connection, included_endpoints=included,
        view_proofs=view_proofs if schema_grouped_sec_views else None,
    )
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
    # These connections are independent of the segment compactor. Apply the
    # same budget before opening either catalog, including on reuse-only runs.
    database_config = {"threads": threads, "memory_limit": memory_limit}
    previous = _reusable_view_catalog(database_path, config=database_config)
    endpoint_seconds: dict[str, float] = {}
    endpoint_actions: dict[str, str] = {}
    rebuilt_views: dict[str, str] = {}
    expected_catalog: dict[str, tuple[str, str, str | None, str | None]] = {}
    fenced_sources: dict[Path, tuple[int, int, int, int, int]] = {}
    reader_policy: dict[str, object] | None = None
    if schema_grouped_sec_views:
        for path in grouped.get(SCHEMA_GROUPED_ENDPOINT, []):
            proof = (view_proofs or {}).get(str(path))
            fenced_sources[path] = (
                proof.file_signature if proof is not None else _view_file_signature(path)
            )
        _verify_view_sources(fenced_sources)
    try:
        if previous is not None:
            shutil.copy2(database_path, temporary)
        database = duckdb.connect(str(temporary), config=database_config)
        try:
            if previous is None:
                database.execute(
                    "CREATE TABLE l1_catalog ("
                    "endpoint VARCHAR, view_name VARCHAR, query_state VARCHAR, "
                    "query_reason VARCHAR, schema_variants BIGINT, segment_count BIGINT, "
                    "source_files BIGINT, rows BIGINT, input_bytes BIGINT, "
                    "output_bytes BIGINT, updated_at_utc VARCHAR, "
                    "view_input_signature VARCHAR, view_sql_signature VARCHAR)"
                )
            else:
                database.execute("DELETE FROM l1_catalog")
                for old_endpoint, (old_state, old_name, _, _) in previous.items():
                    if old_state == "published" and (
                        old_endpoint not in endpoint_statistics
                        or old_endpoint in deferred
                    ):
                        database.execute(f"DROP VIEW {old_name}")
            for endpoint, totals in endpoint_statistics.items():
                endpoint_started = time.monotonic()
                paths = grouped.get(endpoint, [])
                reason = deferred.get(endpoint)
                view_name: str | None = None
                view_signature: str | None = None
                view_sql_signature: str | None = None
                if reason is None:
                    missing = [str(path) for path in paths if not path.is_file()]
                    if missing:
                        raise FileNotFoundError(
                            f"active L1 segment files are missing: {missing[:5]}"
                        )
                    view_name = _view_name(endpoint)
                    view_signature = _view_input_signature(paths)
                    query: str | None = None
                    schema_groups = None
                    if schema_grouped_sec_views and endpoint == SCHEMA_GROUPED_ENDPOINT:
                        schema_groups = _schema_grouped_view(
                            database, paths, database_path.parent, view_proofs or {}
                        )
                        if schema_groups is not None:
                            query, view_signature, reader_policy = schema_groups
                    if previous is not None and previous.get(endpoint, ())[:3] == (
                        "published", view_name, view_signature
                    ):
                        endpoint_actions[endpoint] = (
                            "reused_verified_schema_groups" if schema_groups is not None
                            else "reused_verified_paths"
                        )
                        view_sql_signature = previous[endpoint][3]
                    else:
                        if query is None:
                            query = _parquet_view_query(paths)
                        database.execute(
                            f"CREATE OR REPLACE VIEW {view_name} AS {query}"
                        )
                        endpoint_actions[endpoint] = (
                            "rebuilt_schema_groups" if schema_groups is not None else "rebuilt"
                        )
                        rebuilt_views[endpoint] = view_name
                else:
                    endpoint_actions[endpoint] = "deferred"
                expected_catalog[endpoint] = (
                    "deferred" if reason is not None else "published",
                    view_name or "", view_signature, view_sql_signature,
                )
                database.execute(
                    "INSERT INTO l1_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        endpoint,
                        view_name,
                        "deferred" if reason is not None else "published",
                        reason,
                        int(totals["schema_variants"] or 0),
                        int(totals["segment_count"] or 0),
                        int(totals["source_files"] or 0),
                        int(totals["rows"] or 0),
                        int(totals["input_bytes"] or 0),
                        int(totals["output_bytes"] or 0),
                        str(totals["updated_at"] or ""),
                        view_signature,
                        view_sql_signature,
                    ),
                )
                endpoint_seconds[endpoint] = round(
                    time.monotonic() - endpoint_started, 3
                )
            database.execute("CHECKPOINT")
            if reader_policy is not None and _schema_group_reader_policy(database) != reader_policy:
                raise RuntimeError("L1 grouped-view reader policy changed during publication")
        finally:
            database.close()
        if rebuilt_views:
            # DuckDB can serialize a multi-UNION view differently after its
            # first close/reopen. Sign that persisted SQL, never a normalized
            # string or the writer's pre-serialization representation. Only
            # views created above may be signed; reused views retain the exact
            # signature already validated against the old public catalog.
            database = duckdb.connect(str(temporary), config=database_config)
            try:
                persisted_sql = dict(database.execute(
                    "SELECT view_name, sql FROM duckdb_views() "
                    "WHERE schema_name='main' AND view_name LIKE 'openbb_l1_%'"
                ).fetchall())
                for endpoint, view_name in rebuilt_views.items():
                    view_sql = persisted_sql.get(view_name)
                    if not isinstance(view_sql, str):
                        raise RuntimeError(f"published L1 view is missing: {view_name}")
                    sql_signature = hashlib.sha256(view_sql.encode("utf-8")).hexdigest()
                    database.execute(
                        "UPDATE l1_catalog SET view_sql_signature=? "
                        "WHERE endpoint=? AND view_name=? AND query_state='published'",
                        (sql_signature, endpoint, view_name),
                    )
                    state, name, signature, _ = expected_catalog[endpoint]
                    expected_catalog[endpoint] = (state, name, signature, sql_signature)
                database.execute("CHECKPOINT")
            finally:
                database.close()
            del persisted_sql
        if _reusable_view_catalog(temporary, config=database_config) != expected_catalog:
            raise RuntimeError("persisted L1 view catalog failed signature validation")
        _verify_view_sources(fenced_sources)
        os.replace(temporary, database_path)
    finally:
        temporary.unlink(missing_ok=True)
    return len(grouped), deferred, endpoint_seconds, endpoint_actions


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
        for row in connection.execute(
            sql.format(filter_clause=filter_clause), parameters
        )
    }


def _write_status(
    connection: sqlite3.Connection,
    output_dir: Path,
    filters: Sequence[str],
    *,
    stale_segments: int,
    new_segments: int,
    deferred_query_views: Mapping[str, str],
    stage_seconds: Mapping[str, float] | None = None,
    view_endpoint_seconds: Mapping[str, float] | None = None,
    view_endpoint_actions: Mapping[str, str] | None = None,
    stage_resources: Mapping[str, Mapping[str, int | None]] | None = None,
    source_audit: Mapping[str, object] | None = None,
    schema_grouped_sec_views: bool = False,
    started_at_utc: str | None = None,
    failed_segments: int = 0,
    deferred_failed_segments: int = 0,
) -> dict[str, object]:
    timings = dict(stage_seconds or {})
    status_started = time.monotonic()
    plan_token = _active_plan_token(connection)
    token_clause = "" if plan_token is None else " AND t.plan_token=?"
    filter_clause, filter_parameters = _filter_sql(filters, alias="t")
    task_parameters: list[object] = [] if plan_token is None else [plan_token]
    task_parameters.extend(filter_parameters)
    task_index_hint = (
        f" INDEXED BY {TASK_COMPACTION_INDEX}"
        if plan_token is not None
        and not filters
        and any(
            str(row[1]) == TASK_COMPACTION_INDEX
            for row in connection.execute("PRAGMA index_list(tasks)")
        )
        else ""
    )
    tasks = {
        str(row["endpoint"]): row
        for row in connection.execute(
            f"""
            SELECT t.endpoint, COUNT(*) AS success_files,
                   SUM(t.rows) AS success_rows
            FROM tasks AS t{task_index_hint}
            WHERE t.active=1 AND t.status='success'{token_clause}{filter_clause}
            GROUP BY t.endpoint
            """,
            task_parameters,
        )
    }
    timings["status_task_count"] = round(time.monotonic() - status_started, 3)
    status_started = time.monotonic()
    # Each segment and its source members are committed in the same SQLite
    # transaction, and the compact receipt checks source file/row totals.
    # Avoid grouping millions of members again just to render status. A cheap
    # endpoint-wise member cardinality check still fails closed on missing,
    # extra, or endpoint-misassigned memberships. The existing endpoint index
    # makes this a covering scan without loading member source-row payloads.
    member_filter, member_parameters = _filter_sql(filters, alias="m")
    member_counts = {
        str(row["endpoint"]): int(row["source_files"])
        for row in connection.execute(
            f"SELECT m.endpoint, COUNT(*) AS source_files "
            f"FROM {MEMBER_TABLE} AS m WHERE 1=1{member_filter} "
            "GROUP BY m.endpoint",
            member_parameters,
        )
    }
    timings["status_member_count"] = round(time.monotonic() - status_started, 3)
    status_started = time.monotonic()
    segments = _grouped_counts(
        connection,
        f"""
        SELECT s.endpoint, COUNT(*) AS active_segments,
               SUM(s.source_files) AS compacted_files,
               SUM(s.source_rows) AS compacted_rows,
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
    timings["status_segment_count"] = round(time.monotonic() - status_started, 3)
    for endpoint in sorted(set(member_counts) | set(segments)):
        member_count = member_counts.get(endpoint, 0)
        segment_member_count = (
            int(segments[endpoint]["compacted_files"] or 0)
            if endpoint in segments else 0
        )
        if member_count != segment_member_count:
            raise RuntimeError(
                "L1 status member/segment count mismatch: "
                f"endpoint={endpoint} members={member_count} "
                f"segment_source_files={segment_member_count}"
            )
    status_started = time.monotonic()
    endpoint_rows: list[dict[str, object]] = []
    for endpoint in sorted(set(tasks) | set(segments)):
        task = tasks.get(endpoint)
        segment = segments.get(endpoint)
        success_files = int(task["success_files"] or 0) if task is not None else 0
        success_rows = int(task["success_rows"] or 0) if task is not None else 0
        compacted_files = (
            int(segment["compacted_files"] or 0) if segment is not None else 0
        )
        compacted_rows = (
            int(segment["compacted_rows"] or 0) if segment is not None else 0
        )
        source_bytes = int(segment["source_bytes"] or 0) if segment is not None else 0
        output_bytes = int(segment["output_bytes"] or 0) if segment is not None else 0
        endpoint_rows.append(
            {
                "endpoint": endpoint,
                "view_name": (
                    _view_name(endpoint)
                    if segment is not None and endpoint not in deferred_query_views
                    else None
                ),
                "query_view_state": (
                    "deferred"
                    if endpoint in deferred_query_views
                    else "published"
                    if segment is not None
                    else "unavailable"
                ),
                "query_view_reason": deferred_query_views.get(endpoint),
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
                "query_view_state": pl.String,
                "query_view_reason": pl.String,
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
    timings["status_projection_write"] = round(time.monotonic() - status_started, 3)
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "started_at_utc": started_at_utc,
        "finished_at_utc": generated_at,
        "systemd_invocation_id": os.environ.get("INVOCATION_ID"),
        "state": (
            "completed_with_failures" if failed_segments else
            "completed_with_deferred_segments" if deferred_failed_segments else "completed"
        ),
        "failed_segments": failed_segments,
        "deferred_failed_segments": deferred_failed_segments,
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
        "stale_source_scan_order": (
            "journal_task_id"
            if source_audit and source_audit.get("mode") == "incremental"
            else STALE_SOURCE_SCAN_ORDER
        ),
        "source_audit": dict(source_audit or {"mode": "full_legacy"}),
        "deferred_query_views": dict(sorted(deferred_query_views.items())),
        "stage_seconds": timings,
        "view_endpoint_seconds": dict(sorted((view_endpoint_seconds or {}).items())),
        "view_endpoint_actions": dict(sorted((view_endpoint_actions or {}).items())),
        "query_view_policy": {
            "schema_grouped_sec_requested": schema_grouped_sec_views,
            "schema_grouped_policy_revision": SCHEMA_GROUPED_POLICY_REVISION,
            "physical_identity_revision": SCHEMA_IDENTITY_REVISION,
        },
        "stage_resources": dict(stage_resources or {}),
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
    query_states: dict[str, str] = {}
    if database is not None:
        try:
            query_states = {
                str(endpoint): str(query_state)
                for endpoint, query_state in database.execute(
                    "SELECT endpoint, query_state FROM l1_catalog"
                ).fetchall()
            }
        except duckdb.Error:
            # Legacy catalogs have no query_state column. Treat their endpoint
            # views as published so the existing audit contract is preserved.
            query_states = {}
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
                    source_metadata = pq.ParquetFile(source_path).metadata
                    source_file_rows = int(source_metadata.num_rows)
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
                if (
                    parquet_schema_fingerprint(output_path)
                    != segment["schema_fingerprint"]
                ):
                    raise RuntimeError("output schema fingerprint mismatch")
                if database is None:
                    raise RuntimeError("openbb_l1.duckdb is missing")
                query_state = query_states.get(endpoint, "published")
                if query_state not in {"published", "deferred"}:
                    raise RuntimeError(
                        f"unexpected DuckDB query state: {query_state!r}"
                    )
                if query_state == "published" and endpoint not in view_rows:
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
    attempt = _CompactionAttempt(args)
    try:
        return _run_compaction(args, attempt)
    except BaseException as exc:
        if not attempt.finalized:
            # Lock acquisition failed: never overwrite another writer's latest.
            try:
                attempt.fail(exc)
                attempt.finish(persist=False)
            except Exception:
                pass  # Even resource sampling can fail during a process OOM.
        raise


def _run_compaction(args: argparse.Namespace, attempt: _CompactionAttempt) -> int:
    started_at_utc = attempt.payload["started_at_utc"]
    output_dir = args.output_dir.resolve()
    state_dir = output_dir / "_state"
    state_path = state_dir / "openbb_archive.sqlite3"
    lock_path = state_dir / "openbb_l1_compaction.lock"
    with _exclusive_lock(lock_path), attempt.record():
        attempt.phase("archive_guard")
        if args.archive_idle_only:
            allowed, reason = _archive_compaction_allowed(state_dir)
            if not allowed:
                attempt.payload.update(state="deferred", reason="archive_busy", exit_code=0)
                print(f"[openbb-l1] skipped=archive_busy reason={reason}", flush=True)
                return 0
        attempt.phase("manifest_open")
        cleaned_temp_files = _clean_duckdb_temp_files(state_dir / "duckdb_l1_tmp")
        manifest = _open_manifest(state_path)
        stage_seconds = attempt.payload["stage_seconds"]
        stage_resources = attempt.payload["stage_resources"]
        journal_plan: SourceAuditPlan | None = None
        view_proofs: dict[str, ViewSourceProof] | None = (
            {} if args.schema_grouped_sec_views else None
        )
        try:
            if args.audit_only:
                attempt.phase("segment_audit")
                audits = _audit_segments(
                    manifest,
                    output_dir,
                    args.endpoint,
                    show_progress=not args.no_progress,
                )
                _write_audit(output_dir, audits)
                failed = sum(row.status == "failed" for row in audits)
                attempt.payload.update(
                    state="audit_failed" if failed else "audit_completed",
                    failed_segments=failed, exit_code=2 if failed else 0,
                )
                print(
                    "[openbb-l1-audit] "
                    f"segments={len(audits)} passed={len(audits) - failed} "
                    f"failed={failed}",
                    flush=True,
                )
                return 0 if failed == 0 else 2

            if args.incremental_source_audit:
                attempt.phase("source_journal_prepare")
                journal_started = time.monotonic()
                journal_plan = prepare_source_audit(
                    manifest,
                    state_path,
                    contract_fingerprint=_source_audit_fingerprint(),
                    force_full=args.force_full_source_audit,
                )
                stage_seconds["source_journal_prepare"] = round(
                    time.monotonic() - journal_started, 3
                )
            stage_resources["before_stale_contract_audit"] = _stage_resources()
            attempt.phase("stale_contract_audit")
            stage_started = time.monotonic()
            stale_endpoints, stale_segments = _mark_stale_segments(
                manifest,
                args.endpoint,
                changed_task_ids=(
                    journal_plan.task_ids
                    if journal_plan is not None and journal_plan.mode == "incremental"
                    else None
                ),
                timing=stage_seconds,
                view_proofs=view_proofs,
            )
            stage_seconds["stale_contract_audit"] = round(time.monotonic() - stage_started, 3)
            attempt.payload["stale_segments"] = stale_segments
            if journal_plan is not None:
                attempt.phase("source_journal_checkpoint")
                checkpoint_started = time.monotonic()
                finish_source_audit(manifest, state_path, journal_plan)
                stage_seconds["source_journal_checkpoint"] = round(
                    time.monotonic() - checkpoint_started, 3
                )
            stage_resources["after_stale_contract_audit"] = _stage_resources()
            attempt.phase("unassigned_source_load")
            stage_started = time.monotonic()
            shards = _load_unassigned_shards(
                manifest,
                args.endpoint,
                limit=args.max_source_files,
                show_progress=not args.no_progress,
                timing=stage_seconds,
            )
            stage_seconds["unassigned_source_load"] = round(time.monotonic() - stage_started, 3)
            stage_resources["after_unassigned_source_load"] = _stage_resources()
            attempt.phase("batch_planning")
            stage_started = time.monotonic()
            batches = list(
                _segment_batches(
                    shards,
                    max_files=args.max_files_per_segment,
                    max_bytes=args.max_source_bytes_per_segment,
                    max_uncompressed_bytes=(args.max_uncompressed_bytes_per_segment),
                    max_rows=args.max_source_rows_per_segment,
                    min_files=args.min_files_per_segment,
                    include_tail=args.include_tail,
                    flush_endpoints=stale_endpoints,
                )
            )
            stage_seconds["batch_planning"] = round(time.monotonic() - stage_started, 3)
            progress = tqdm(
                batches,
                desc="openbb:l1 compact",
                unit="segment",
                disable=args.no_progress,
            )
            new_segments = 0
            failed_segments = 0
            deferred_failed_segments = 0
            stopped_for_archive = False
            attempt.phase("segment_build")
            stage_started = time.monotonic()
            for batch in progress:
                if args.archive_idle_only:
                    allowed, reason = _archive_compaction_allowed(state_dir)
                    if not allowed:
                        stopped_for_archive = True
                        print(
                            "[openbb-l1] paused=archive_resumed "
                            f"reason={reason} new_segments={new_segments}",
                            flush=True,
                        )
                        break
                endpoint = batch[0].endpoint
                contracts = [item.source_contract() for item in batch]
                signature = source_signature(contracts)
                retry_ready, retry_at = _failure_retry_ready(manifest, signature)
                if not retry_ready:
                    deferred_failed_segments += 1
                    attempt.payload["deferred_failed_segments"] = deferred_failed_segments
                    progress.set_postfix(
                        endpoint=endpoint,
                        deferred_failure=retry_at,
                        refresh=False,
                    )
                    continue
                try:
                    segment_id = _available_segment_id(manifest, endpoint, signature)
                    output_path = _segment_output(output_dir, endpoint, segment_id)
                    for stale_temp in output_path.parent.glob(
                        f".{output_path.name}.*.tmp"
                    ):
                        stale_temp.unlink(missing_ok=True)
                    receipt = compact_parquet_files(
                        [item.output_path for item in batch],
                        output_path,
                        expected_rows=sum(item.rows for item in batch),
                        threads=args.threads,
                        memory_limit=args.memory_limit,
                        compression="zstd",
                        row_group_size_rows=args.row_group_size,
                        temp_directory=state_dir / "duckdb_l1_tmp",
                        source_metadata_proofs=[item.metadata_proof for item in batch],
                        before_publish=lambda: _verify_source_tasks(
                            manifest, endpoint, batch, verify_files=False
                        ),
                    )
                    try:
                        new_proof = None
                        if view_proofs is not None and endpoint == SCHEMA_GROUPED_ENDPOINT:
                            # Old segments reuse the mandatory audit above. Only
                            # newly built, bounded segments need another footer.
                            before = _view_file_signature(output_path)
                            new_proof = _view_source_proof(
                                output_path, segment_id, pq.ParquetFile(str(output_path)),
                                before, output_rows=receipt.output_rows,
                                output_bytes=receipt.output_bytes,
                                arrow_fingerprint=receipt.schema_fingerprint,
                            )
                        _record_segment(
                            manifest,
                            endpoint,
                            segment_id,
                            signature,
                            batch,
                            receipt,
                        )
                        if new_proof is not None:
                            view_proofs[new_proof.path] = new_proof
                    except Exception:
                        # Publication reached the filesystem but not the manifest.
                        # Preserve the file for diagnosis instead of deleting data.
                        orphan = _quarantine_path(output_dir, segment_id, output_path)
                        orphan.parent.mkdir(parents=True, exist_ok=True)
                        if output_path.exists():
                            os.replace(output_path, orphan)
                        raise
                except (MemoryError, duckdb.OutOfMemoryException):
                    # A process-wide resource failure must not poison a healthy
                    # source batch's durable failure/backoff record.
                    raise
                except Exception as exc:
                    failed_segments += 1
                    attempt.payload["failed_segments"] = failed_segments
                    retry_at = _record_compaction_failure(
                        manifest,
                        signature=signature,
                        endpoint=endpoint,
                        source_files=len(batch),
                        error=exc,
                    )
                    print(
                        "[openbb-l1] isolated_failed_segment "
                        f"endpoint={endpoint} files={len(batch)} "
                        f"retry_at={retry_at} error={type(exc).__name__}: "
                        f"{str(exc)[:500]}",
                        flush=True,
                    )
                    continue
                new_segments += 1
                attempt.payload["new_segments"] = new_segments
                _clear_compaction_failure(manifest, signature)
                progress.set_postfix(
                    endpoint=endpoint,
                    files=receipt.source_files,
                    rows=receipt.output_rows,
                    refresh=False,
                )
            stage_seconds["segment_build"] = round(time.monotonic() - stage_started, 3)
            stage_resources["after_segment_build"] = _stage_resources()

            if stopped_for_archive:
                attempt.payload.update(state="deferred", reason="archive_resumed", exit_code=0)
                print(
                    "[openbb-l1] deferred_publication=archive_busy "
                    f"new_segments={new_segments} failed_segments={failed_segments} "
                    f"cleaned_temp_files={cleaned_temp_files}",
                    flush=True,
                )
                return 0

            attempt.phase("query_view_publish")
            attempt.payload["publication_stage"] = "query_publish_started"
            stage_started = time.monotonic()
            (
                view_count,
                deferred_query_views,
                view_endpoint_seconds,
                view_endpoint_actions,
            ) = _publish_views(
                manifest,
                output_dir / "openbb_l1.duckdb",
                threads=args.threads,
                memory_limit=args.memory_limit,
                schema_grouped_sec_views=args.schema_grouped_sec_views,
                view_proofs=view_proofs,
            )
            attempt.payload["publication_stage"] = "query_published"
            stage_seconds["query_view_publish"] = round(time.monotonic() - stage_started, 3)
            stage_resources["after_query_view_publish"] = _stage_resources()
            attempt.phase("stale_output_quarantine")
            stage_started = time.monotonic()
            quarantined_segments = _quarantine_stale_outputs(
                manifest, output_dir, args.endpoint
            )
            stage_seconds["stale_output_quarantine"] = round(time.monotonic() - stage_started, 3)
            attempt.phase("status_publish")
            attempt.payload["publication_stage"] = "status_publish_started"
            status_publish_started = time.monotonic()
            status = _write_status(
                manifest,
                output_dir,
                (),
                stale_segments=stale_segments,
                new_segments=new_segments,
                deferred_query_views=deferred_query_views,
                stage_seconds=stage_seconds,
                view_endpoint_seconds=view_endpoint_seconds,
                view_endpoint_actions=view_endpoint_actions,
                # The published data receipt keeps its existing stage contract;
                # attempt start/end observations belong only to attempt evidence.
                stage_resources={
                    key: value for key, value in stage_resources.items()
                    if key not in {"before_attempt", "after_attempt"}
                },
                schema_grouped_sec_views=args.schema_grouped_sec_views,
                source_audit=(
                    {
                        "mode": journal_plan.mode,
                        "reason": journal_plan.reason,
                        "changed_task_ids": len(journal_plan.task_ids),
                        "cutoff_seq": journal_plan.cutoff_seq,
                    }
                    if journal_plan is not None else None
                ),
                started_at_utc=started_at_utc,
                failed_segments=failed_segments,
                deferred_failed_segments=deferred_failed_segments,
            )
            stage_seconds.update(status["stage_seconds"])
            stage_seconds["status_publish"] = round(time.monotonic() - status_publish_started, 6)
            attempt.payload.update(
                state=status["state"], exit_code=0, publication_stage="status_published",
                **{key: status[key] for key in (
                    "pending_files", "pending_rows", "compacted_files", "compacted_rows",
                    "success_files", "success_rows", "active_segments", "endpoints",
                    "source_bytes", "output_bytes", "query_view_policy",
                ) if key in status},
            )
            # Preserve the monitor's fixed SEC projection without an unbounded
            # endpoint catalog in the journal event.
            for key in ("view_endpoint_seconds", "view_endpoint_actions"):
                values = status.get(key, {})
                attempt.payload[key] = {
                    SCHEMA_GROUPED_ENDPOINT: values[SCHEMA_GROUPED_ENDPOINT]
                } if SCHEMA_GROUPED_ENDPOINT in values else {}
            print(
                "[openbb-l1] "
                f"new_segments={new_segments} stale_segments={stale_segments} "
                f"failed_segments={failed_segments} "
                f"deferred_failed_segments={deferred_failed_segments} "
                f"cleaned_temp_files={cleaned_temp_files} "
                f"quarantined_segments={quarantined_segments} "
                f"views={view_count} compacted_files={status['compacted_files']} "
                f"deferred_query_views={len(deferred_query_views)} "
                f"pending_files={status['pending_files']} l0_deleted=false",
                flush=True,
            )
            return 0
        finally:
            manifest.close()


if __name__ == "__main__":
    raise SystemExit(run())
