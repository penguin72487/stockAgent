from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import http.client
import json
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from collections.abc import Callable, Iterator
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    PersistentProgress,
    SharedRateLimiter,
    atomic_write_text,
    load_env_file,
    provider_rate_limit,
    retry_delay_seconds,
)


BASE_URL = "https://api.dune.com/api/v1"
SCHEMA_VERSION = 1
TERMINAL_FAILURE_STATES = {
    "QUERY_STATE_FAILED",
    "QUERY_STATE_CANCELLED",
    "QUERY_STATE_EXPIRED",
}


@dataclass(frozen=True, slots=True)
class QueryContract:
    query_id: str
    dune_query_id: int
    sql_path: Path
    sql: str
    sql_sha256: str
    fact_family: str
    primary_key: tuple[str, ...]
    event_time_column: str
    expected_columns: tuple[str, ...]
    history_start: date
    chunk_months: int
    cadence_seconds: int
    performance: str


@dataclass(frozen=True, slots=True)
class Partition:
    contract: QueryContract
    start: date
    end: date

    @property
    def partition_id(self) -> str:
        # The right edge of the open/current partition advances every day.
        # Key by its stable calendar start so daily refreshes replace that one
        # partition instead of accumulating overlapping files and receipts.
        return self.start.isoformat()

    @property
    def legacy_partition_id(self) -> str:
        return f"{self.start.isoformat()}_{self.end.isoformat()}"


@dataclass(slots=True)
class PartitionResult:
    query_id: str
    partition_id: str
    status: str
    rows: int
    execution_id: str | None = None
    parquet_path: str | None = None
    receipt_path: str | None = None
    message: str | None = None


class DuneCreditsExhausted(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute only registered, versioned Dune SQL contracts and persist "
            "resumable raw/result partitions with point-in-time lineage."
        )
    )
    parser.add_argument("--config", type=Path, default=Path("configs/dune_crypto_queries.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("data_dune_crypto"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--queries", nargs="*", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--page-size", type=int, default=1_000)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-base", type=float, default=1.0)
    parser.add_argument(
        "--max-partitions",
        type=int,
        default=0,
        help="0 runs every due partition; positive values are for bounded smoke tests.",
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def _today(value: str) -> date:
    text = str(value).strip().lower()
    return date.today() if text in {"today", "now"} else date.fromisoformat(text)


def _add_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 + max(1, int(months))
    year, month0 = divmod(absolute, 12)
    month = month0 + 1
    if month == 12:
        following = date(year + 1, 1, 1)
    else:
        following = date(year, month + 1, 1)
    return date(year, month, min(value.day, (following - date.resolution).day))


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _atomic_parquet(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        pq.write_table(frame.to_arrow(), temporary, compression="zstd", write_statistics=True)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_contracts(config_path: Path, selected: set[str] | None) -> list[QueryContract]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    config_root = config_path.parent
    contracts: list[QueryContract] = []
    for raw in payload.get("queries", []):
        query_id = str(raw.get("id") or "").strip()
        if not raw.get("enabled", False) or (selected is not None and query_id not in selected):
            continue
        if raw.get("execution_mode") != "sql_file":
            raise ValueError(f"{query_id}: only execution_mode=sql_file is allowed")
        sql_path = (config_root / str(raw.get("sql_file") or "")).resolve()
        try:
            sql_path.relative_to(config_root.resolve())
        except ValueError as exc:
            raise ValueError(f"{query_id}: sql_file escapes the config directory") from exc
        sql = sql_path.read_text(encoding="utf-8")
        if sql.count("{{start_date}}") == 0 or sql.count("{{end_date}}") == 0:
            raise ValueError(f"{query_id}: SQL must contain start_date and end_date placeholders")
        primary_key = tuple(str(value) for value in raw.get("primary_key", []))
        expected = tuple(str(value) for value in raw.get("expected_columns", []))
        event_column = str(raw.get("event_time_column") or "")
        if not query_id or not primary_key or event_column not in expected:
            raise ValueError(f"{query_id or '<missing-id>'}: invalid schema contract")
        contracts.append(
            QueryContract(
                query_id=query_id,
                dune_query_id=int(raw.get("query_id") or 0),
                sql_path=sql_path,
                sql=sql,
                sql_sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                fact_family=str(raw.get("fact_family") or query_id),
                primary_key=primary_key,
                event_time_column=event_column,
                expected_columns=expected,
                history_start=date.fromisoformat(str(raw["history_start"])),
                chunk_months=max(1, int(raw.get("chunk_months") or 3)),
                cadence_seconds=max(60, int(raw.get("cadence_seconds") or 86400)),
                performance=str(raw.get("performance") or "medium"),
            )
        )
    if selected:
        missing = sorted(selected - {item.query_id for item in contracts})
        if missing:
            raise ValueError(f"unknown or disabled Dune query contracts: {', '.join(missing)}")
    return contracts


def _receipt_path(output_dir: Path, partition: Partition) -> Path:
    return output_dir / "receipts" / partition.contract.query_id / f"{partition.partition_id}.json"


def _parquet_path(output_dir: Path, partition: Partition) -> Path:
    return (
        output_dir
        / "normalized"
        / partition.contract.fact_family
        / f"year={partition.start.year:04d}"
        / f"{partition.partition_id}.parquet"
    )


def _execution_state_path(output_dir: Path, partition: Partition) -> Path:
    return (
        output_dir
        / "state"
        / "executions"
        / partition.contract.query_id
        / f"{partition.partition_id}.json"
    )


def _migrate_legacy_partition(output_dir: Path, partition: Partition) -> None:
    """Adopt pre-stable-ID receipts without redownloading completed queries."""

    new_receipt = _receipt_path(output_dir, partition)
    if new_receipt.is_file():
        return
    receipt_root = new_receipt.parent
    for candidate in sorted(receipt_root.glob(f"{partition.start.isoformat()}_*.json")):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if (
            payload.get("status") != "complete"
            or payload.get("sql_sha256") != partition.contract.sql_sha256
            or payload.get("window_start") != partition.start.isoformat()
            or payload.get("window_end_exclusive") != partition.end.isoformat()
        ):
            continue
        old_parquet = Path(str(payload.get("parquet_path") or ""))
        new_parquet = _parquet_path(output_dir, partition)
        if old_parquet.is_file() and old_parquet != new_parquet:
            new_parquet.parent.mkdir(parents=True, exist_ok=True)
            os.replace(old_parquet, new_parquet)
        if not new_parquet.is_file():
            continue
        payload["partition_id"] = partition.partition_id
        payload["parquet_path"] = str(new_parquet)
        payload["migrated_from_partition_id"] = candidate.stem
        _atomic_json(new_receipt, payload)
        candidate.rename(candidate.with_suffix(".json.migrated"))

        old_state = (
            output_dir
            / "state"
            / "executions"
            / partition.contract.query_id
            / f"{candidate.stem}.json"
        )
        new_state = _execution_state_path(output_dir, partition)
        if old_state.is_file() and not new_state.is_file():
            try:
                state_payload = json.loads(old_state.read_text(encoding="utf-8"))
                state_payload["partition_id"] = partition.partition_id
                state_payload["receipt_path"] = str(new_receipt)
                state_payload["migrated_from_partition_id"] = candidate.stem
                _atomic_json(new_state, state_payload)
                old_state.rename(old_state.with_suffix(".json.migrated"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                pass
        return


def _upgrade_partition_lineage(output_dir: Path, partition: Partition) -> None:
    """Correct pre-v2 direct-SQL lineage without spending another Dune credit."""

    receipt_path = _receipt_path(output_dir, partition)
    parquet_path = _parquet_path(output_dir, partition)
    if not receipt_path.is_file() or not parquet_path.is_file():
        return
    try:
        schema_names = set(pq.read_schema(parquet_path).names)
    except (OSError, ValueError):
        return
    if "_dune_contract_id" not in schema_names:
        frame = pl.read_parquet(parquet_path)
        frame = frame.with_columns(
            pl.lit(partition.contract.query_id).alias("_dune_contract_id"),
            pl.lit(partition.contract.dune_query_id).alias("_dune_query_id"),
        )
        _atomic_parquet(parquet_path, frame)
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return
    if (
        receipt.get("contract_id") != partition.contract.query_id
        or receipt.get("dune_query_id") != partition.contract.dune_query_id
    ):
        receipt["contract_id"] = partition.contract.query_id
        receipt["dune_query_id"] = partition.contract.dune_query_id
        receipt["lineage_upgraded_at_utc"] = datetime.now(UTC).isoformat()
        _atomic_json(receipt_path, receipt)


def _partition_due(output_dir: Path, partition: Partition, *, force: bool, today: date) -> bool:
    _migrate_legacy_partition(output_dir, partition)
    _upgrade_partition_lineage(output_dir, partition)
    if force:
        return True
    receipt_path = _receipt_path(output_dir, partition)
    parquet_path = _parquet_path(output_dir, partition)
    if not receipt_path.is_file() or not parquet_path.is_file():
        return True
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        completed = datetime.fromisoformat(str(receipt["completed_at_utc"]).replace("Z", "+00:00"))
    except (OSError, UnicodeError, ValueError, KeyError, json.JSONDecodeError):
        return True
    if receipt.get("status") != "complete" or receipt.get("sql_sha256") != partition.contract.sql_sha256:
        return True
    if partition.end < today:
        return False
    return (datetime.now(UTC) - completed.astimezone(UTC)).total_seconds() >= partition.contract.cadence_seconds


def _build_partitions(
    contracts: list[QueryContract],
    *,
    requested_start: date | None,
    end: date,
) -> list[Partition]:
    output: list[Partition] = []
    for contract in contracts:
        cursor = max(contract.history_start, requested_start) if requested_start else contract.history_start
        while cursor < end:
            boundary = min(_add_months(cursor, contract.chunk_months), end)
            output.append(Partition(contract, cursor, boundary))
            cursor = boundary
    return output


class DuneClient:
    def __init__(
        self,
        api_key: str,
        *,
        max_retries: int,
        retry_base: float,
        poll_seconds: float,
        page_size: int,
    ) -> None:
        self.api_key = api_key
        self.max_retries = max(0, int(max_retries))
        self.retry_base = max(0.1, float(retry_base))
        self.poll_seconds = max(1.0, float(poll_seconds))
        # A 4k-row JSON result repeatedly produced an HTTP/1 partial body in
        # live acceptance while 1k pages were stable.  Keep pagination bounded
        # even though the API accepts larger values.
        self.page_size = min(1_000, max(1, int(page_size)))
        low = provider_rate_limit("dune_free_low")
        high = provider_rate_limit("dune_free_high")
        self.low_limiter = SharedRateLimiter(low.interval_seconds, name=low.provider)
        self.high_limiter = SharedRateLimiter(high.interval_seconds, name=high.provider)

    def _request(self, method: str, path: str, *, body: Any = None, high: bool) -> dict[str, Any]:
        encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        limiter = self.high_limiter if high else self.low_limiter
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            limiter.wait()
            request = Request(
                f"{BASE_URL}{path}",
                data=encoded,
                method=method,
                headers={
                    "X-Dune-API-Key": self.api_key,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "stockAgent-dune-history/1",
                },
            )
            try:
                with urlopen(request, timeout=120) as response:
                    decoded = json.loads(response.read())
                if not isinstance(decoded, dict):
                    raise RuntimeError("Dune response is not an object")
                return decoded
            except HTTPError as exc:
                last_error = exc
                if exc.code == 402:
                    raise DuneCreditsExhausted("Dune returned HTTP 402; no additional executions will be started") from exc
                if exc.code in {408, 429, 500, 502, 503, 504} and attempt < self.max_retries:
                    limiter.defer(retry_delay_seconds(attempt, base=self.retry_base, retry_after=exc.headers.get("Retry-After")))
                    continue
                detail = exc.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"Dune HTTP {exc.code}: {detail}") from exc
            except (
                URLError,
                TimeoutError,
                json.JSONDecodeError,
                http.client.IncompleteRead,
                ConnectionError,
            ) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    limiter.defer(retry_delay_seconds(attempt, base=self.retry_base))
                    continue
                raise
        raise last_error or RuntimeError("Dune request failed")

    def execute(self, sql: str, performance: str) -> str:
        payload = self._request(
            "POST",
            "/sql/execute",
            body={"sql": sql, "performance": performance},
            high=False,
        )
        execution_id = str(payload.get("execution_id") or "").strip()
        if not execution_id:
            raise RuntimeError(f"Dune execute response lacks execution_id: {payload}")
        return execution_id

    def wait_complete(
        self,
        execution_id: str,
        *,
        heartbeat: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        while True:
            if heartbeat is not None:
                heartbeat()
            payload = self._request("GET", f"/execution/{execution_id}/status", high=True)
            state = str(payload.get("state") or "")
            if state == "QUERY_STATE_COMPLETED":
                return payload
            if state in TERMINAL_FAILURE_STATES:
                raise RuntimeError(f"Dune execution {execution_id} ended in {state}: {payload}")
            time.sleep(self.poll_seconds)

    def results(self, execution_id: str) -> Iterator[tuple[int, dict[str, Any]]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            query = urlencode({"limit": self.page_size, "offset": offset})
            payload = self._request("GET", f"/execution/{execution_id}/results?{query}", high=True)
            result = payload.get("result") or {}
            page_rows = result.get("rows") or []
            if not isinstance(page_rows, list):
                raise RuntimeError("Dune result rows are not a list")
            rows.extend(row for row in page_rows if isinstance(row, dict))
            yield offset, payload
            next_offset = payload.get("next_offset")
            if next_offset is None:
                next_offset = result.get("next_offset")
            if next_offset is None and len(page_rows) >= self.page_size:
                next_offset = offset + len(page_rows)
            if next_offset is None or not page_rows:
                break
            offset = int(next_offset)


def _render_sql(partition: Partition) -> str:
    return (
        partition.contract.sql.replace("{{start_date}}", partition.start.isoformat())
        .replace("{{end_date}}", partition.end.isoformat())
    )


def _validate_frame(frame: pl.DataFrame, partition: Partition) -> pl.DataFrame:
    contract = partition.contract
    missing = sorted(set(contract.expected_columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{contract.query_id}: result is missing columns: {', '.join(missing)}")
    frame = frame.select(list(contract.expected_columns))
    if frame.is_empty():
        return frame
    null_pk = frame.select(
        pl.any_horizontal(
            [pl.col(name).is_null() for name in contract.primary_key]
        ).any()
    ).item()
    if null_pk:
        raise RuntimeError(f"{contract.query_id}: primary key contains null values")
    duplicate_count = frame.select(pl.struct(contract.primary_key).is_duplicated().sum()).item()
    if int(duplicate_count or 0) > 0:
        raise RuntimeError(f"{contract.query_id}: {duplicate_count} duplicate primary-key rows")
    event_dates = frame.get_column(contract.event_time_column).cast(pl.String).str.to_date(strict=False)
    if event_dates.null_count() or frame.filter((event_dates < partition.start) | (event_dates >= partition.end)).height:
        raise RuntimeError(f"{contract.query_id}: event dates escape partition [{partition.start}, {partition.end})")
    return frame


def _run_partition(
    client: DuneClient,
    output_dir: Path,
    partition: Partition,
    *,
    heartbeat: Callable[[], None] | None = None,
) -> PartitionResult:
    contract = partition.contract
    rendered_sql = _render_sql(partition)
    execution_state_path = _execution_state_path(output_dir, partition)
    execution_id = ""
    if execution_state_path.is_file():
        try:
            execution_state = json.loads(
                execution_state_path.read_text(encoding="utf-8")
            )
            if execution_state.get("sql_sha256") == contract.sql_sha256:
                execution_id = str(execution_state.get("execution_id") or "")
        except (OSError, UnicodeError, json.JSONDecodeError):
            execution_id = ""
    if not execution_id:
        execution_id = client.execute(rendered_sql, contract.performance)
        _atomic_json(
            execution_state_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "submitted",
                "query_id": contract.query_id,
                "partition_id": partition.partition_id,
                "execution_id": execution_id,
                "sql_sha256": contract.sql_sha256,
                "submitted_at_utc": datetime.now(UTC).isoformat(),
            },
        )
    status_payload = client.wait_complete(execution_id, heartbeat=heartbeat)
    retrieved_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    raw_root = output_dir / "raw" / contract.query_id / partition.partition_id / execution_id
    page_count = 0
    for offset, payload in client.results(execution_id):
        page_rows = (payload.get("result") or {}).get("rows") or []
        rows.extend(row for row in page_rows if isinstance(row, dict))
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        _atomic_bytes(raw_root / f"page_{offset:012d}.json.gz", gzip.compress(encoded, compresslevel=6, mtime=0))
        page_count += 1
    frame = pl.from_dicts(rows, infer_schema_length=None, strict=False) if rows else pl.DataFrame(schema={name: pl.String for name in contract.expected_columns})
    frame = _validate_frame(frame, partition)
    available_at = str(status_payload.get("execution_ended_at") or retrieved_at.isoformat())
    frame = frame.with_columns(
        pl.lit(execution_id).alias("_dune_execution_id"),
        pl.lit(contract.query_id).alias("_dune_contract_id"),
        pl.lit(contract.dune_query_id).alias("_dune_query_id"),
        pl.lit(contract.sql_sha256).alias("_dune_sql_sha256"),
        pl.lit(available_at).alias("_available_at_utc"),
        pl.lit(retrieved_at.isoformat()).alias("_retrieved_at_utc"),
        pl.lit(partition.start.isoformat()).alias("_window_start"),
        pl.lit(partition.end.isoformat()).alias("_window_end"),
    )
    parquet_path = _parquet_path(output_dir, partition)
    _atomic_parquet(parquet_path, frame)
    receipt_path = _receipt_path(output_dir, partition)
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "status": "complete",
        "query_id": contract.query_id,
        "contract_id": contract.query_id,
        "dune_query_id": contract.dune_query_id,
        "fact_family": contract.fact_family,
        "partition_id": partition.partition_id,
        "window_start": partition.start.isoformat(),
        "window_end_exclusive": partition.end.isoformat(),
        "execution_id": execution_id,
        "sql_path": str(contract.sql_path),
        "sql_sha256": contract.sql_sha256,
        "expected_columns": list(contract.expected_columns),
        "primary_key": list(contract.primary_key),
        "rows": frame.height,
        "pages": page_count,
        "available_at_utc": available_at,
        "completed_at_utc": retrieved_at.isoformat(),
        "parquet_path": str(parquet_path),
        "raw_root": str(raw_root),
    }
    _atomic_json(receipt_path, receipt)
    _atomic_json(
        execution_state_path,
        {
            "schema_version": SCHEMA_VERSION,
            "status": "complete",
            "query_id": contract.query_id,
            "partition_id": partition.partition_id,
            "execution_id": execution_id,
            "sql_sha256": contract.sql_sha256,
            "completed_at_utc": retrieved_at.isoformat(),
            "receipt_path": str(receipt_path),
        },
    )
    return PartitionResult(contract.query_id, partition.partition_id, "complete", frame.height, execution_id, str(parquet_path), str(receipt_path))


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    config_path = args.config if args.config.is_absolute() else repo_root / args.config
    output_dir = args.output_dir if args.output_dir.is_absolute() else repo_root / args.output_dir
    env_file = args.env_file if args.env_file.is_absolute() else repo_root / args.env_file
    load_env_file(env_file, allowed_names={"DUNE_API_KEY"})
    api_key = os.getenv("DUNE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("DUNE_API_KEY is not configured; no Dune request was sent")
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (output_dir / ".download.lock").open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"[dune] another updater owns {output_dir / '.download.lock'}; skip", flush=True)
        return 0

    selected = set(args.queries) if args.queries else None
    contracts = _load_contracts(config_path, selected)
    end = _today(args.end_date)
    requested_start = date.fromisoformat(args.start_date) if args.start_date else None
    all_partitions = _build_partitions(contracts, requested_start=requested_start, end=end)
    due = [item for item in all_partitions if _partition_due(output_dir, item, force=args.force, today=date.today())]
    if args.max_partitions > 0:
        due = due[: args.max_partitions]
    progress = PersistentProgress(
        output_dir / "progress.json",
        label="Dune crypto history",
        total=len(due),
        unit="partitions",
        basis="ETA uses completed calendar partitions; Dune queueing and credit exhaustion can change it.",
    )
    client = DuneClient(api_key, max_retries=args.max_retries, retry_base=args.retry_base, poll_seconds=args.poll_seconds, page_size=args.page_size)
    results: list[PartitionResult] = []
    stop = threading.Event()

    def worker(partition: Partition) -> PartitionResult:
        if stop.is_set():
            return PartitionResult(partition.contract.query_id, partition.partition_id, "not_started", 0, message="stopped after another partition exhausted credits")
        pulse_stop = threading.Event()
        phase = f"{partition.contract.query_id}:{partition.partition_id}"

        def pulse() -> None:
            while not pulse_stop.wait(5.0):
                progress.heartbeat(phase)

        pulse_thread = threading.Thread(
            target=pulse,
            name=f"dune-progress-{partition.partition_id}",
            daemon=True,
        )
        pulse_thread.start()
        try:
            progress.heartbeat(phase)
            result = _run_partition(
                client,
                output_dir,
                partition,
                heartbeat=lambda: progress.heartbeat(phase),
            )
        except DuneCreditsExhausted as exc:
            stop.set()
            result = PartitionResult(partition.contract.query_id, partition.partition_id, "blocked_credits", 0, message=str(exc))
        except Exception as exc:  # each partition remains independently resumable
            result = PartitionResult(partition.contract.query_id, partition.partition_id, "failed", 0, message=f"{type(exc).__name__}: {exc}")
        finally:
            pulse_stop.set()
        progress.update(f"{partition.contract.query_id}:{partition.partition_id}", result.status)
        return result

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, len(contracts) or 1))) as executor:
        futures = [executor.submit(worker, partition) for partition in due]
        for future in as_completed(futures):
            results.append(future.result())

    failed = any(item.status in {"failed", "blocked_credits", "not_started"} for item in results)
    progress.finish(failed=failed)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "state": "failed" if failed else "complete",
        "registered_queries": len(contracts),
        "registered_partitions": len(all_partitions),
        "due_partitions": len(due),
        "completed_partitions": sum(item.status == "complete" for item in results),
        "failed_partitions": sum(item.status == "failed" for item in results),
        "blocked_credit_partitions": sum(item.status == "blocked_credits" for item in results),
        "rows": sum(item.rows for item in results),
        "results": [asdict(item) for item in sorted(results, key=lambda value: (value.query_id, value.partition_id))],
    }
    _atomic_json(output_dir / "download_summary.json", summary)
    print(json.dumps({key: summary[key] for key in ("state", "due_partitions", "completed_partitions", "failed_partitions", "blocked_credit_partitions", "rows")}, ensure_ascii=False), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
