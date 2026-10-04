from __future__ import annotations

import argparse
import json
import math
import sys
import threading
from concurrent.futures import Future
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import requests

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.common import (
    SharedRateLimiter,
    describe_rate_limit,
    resolve_end_date,
    resolve_request_interval,
    retry_delay_seconds,
    run_parallel_tasks,
)
from downloader.artifact_io import (
    atomic_write_json, atomic_write_parquet, atomic_write_text, sha256_bytes, sha256_file,
)
from downloader.dataset_lock import (
    DEFAULT_LOCK_TIMEOUT_SECONDS, exclusive_dataset_lock, parse_lock_timeout_seconds,
)

API_BASE = "https://api.frankfurter.dev/v1"
HEAD_COVERAGE_VERSION = 2
DEFAULT_SYMBOLS_PATH = Path("data_yahoo") / "forex" / "symbols.csv"
_RATE_LIMITER: SharedRateLimiter | None = None
_HTTP_LOCAL = threading.local()
_MAX_RETRIES = 4
_RETRY_BASE = 0.6
_BASE_RESPONSES: dict[tuple[str, str, str, int], Future] = {}
_BASE_RESPONSE_LOCK = threading.Lock()
_BASE_RESPONSE_CACHE_LIMIT = 8
_HEAD_SOURCE_LOCK = threading.Lock()


class HistoricalSourceError(ValueError):
    """Invalid historical source evidence, not a local Parquet read failure."""


def acquisition_contract() -> dict:
    payload = {"head_coverage_version": HEAD_COVERAGE_VERSION, "api_base": API_BASE,
               "provider": "ECB", "price_schema_version": 1,
               "price_semantics": "unchanged native v1 reference rates; no synthetic rebasing"}
    return {**payload, "fingerprint_sha256": sha256_bytes(json.dumps(payload, sort_keys=True).encode())}


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _read_parquet_row_count(path: Path) -> int:
    return int(pq.ParquetFile(path, memory_map=True).metadata.num_rows)


def _read_date_column(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path, columns=["date"], memory_map=True))


def _write_parquet(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(path, frame, compression="snappy", write_statistics=True)


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_text(path, frame.write_csv())


def _write_text(path: Path, value: str) -> None:
    atomic_write_text(path, value)


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    base: str
    quote: str


@dataclass(slots=True)
class DownloadResult:
    code: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download forex OHLC-like data from Frankfurter (ECB rates)."
    )
    parser.add_argument(
        "--mode",
        choices=["daily-update", "full"],
        default="daily-update",
        help="daily-update: append only missing dates; full: skip existing unless --refresh.",
    )
    parser.add_argument(
        "--start-date", default="1999-01-04", help="Inclusive ECB history start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        default="today",
        help="Inclusive end date in YYYY-MM-DD, or 'today'/'now' to use current local date.",
    )
    parser.add_argument(
        "--output-dir", default="data_forex_frankfurter", help="Output directory"
    )
    parser.add_argument(
        "--symbols-file",
        default=None,
        help="Optional text file with one 6-letter pair per line",
    )
    parser.add_argument("--workers", type=int, default=8, help="Concurrent workers")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument("--lock-timeout-seconds", type=parse_lock_timeout_seconds,
                        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
                        help="Bounded wait for the canonical dataset writer lock, in seconds")
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--retry-base", type=float, default=0.6)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help="Global minimum seconds between API requests. Default uses Frankfurter public profile.",
    )
    parser.add_argument(
        "--refresh", action="store_true", help="Re-download even if parquet exists"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Deprecated compatibility flag. Same as --mode daily-update.",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="Do not overwrite output_dir/symbols.csv",
    )
    parser.add_argument("--official-history", action="store_true",
                        help="Also resume source-separated v2 central-bank history (not blended FX).")
    parser.add_argument("--official-history-max-requests", type=int, default=16)
    return parser.parse_args()


def _get_json(url: str, timeout: int) -> dict:
    session = getattr(_HTTP_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "stockAgent-economic-research/1.0"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _HTTP_LOCAL.session = session
    last_error: Exception | None = None
    for attempt in range(max(0, int(_MAX_RETRIES)) + 1):
        if _RATE_LIMITER is not None:
            _RATE_LIMITER.wait()
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise HistoricalSourceError(f"Unexpected response shape for {url}")
            return data
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retriable = status in {408, 425, 429, 500, 502, 503, 504} or status is None
            if attempt >= _MAX_RETRIES or not retriable:
                raise
            headers = getattr(getattr(exc, "response", None), "headers", {})
            delay = retry_delay_seconds(
                attempt,
                base=_RETRY_BASE,
                retry_after=headers.get("Retry-After"),
            )
            if _RATE_LIMITER is not None:
                _RATE_LIMITER.defer(delay)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Frankfurter request failed without explicit error: {url}")


def _load_supported_currencies(timeout: int) -> set[str]:
    payload = _get_json(f"{API_BASE}/currencies", timeout)
    return {str(code).upper() for code in payload.keys()}


def _base_rates(base: str, start: str, end: str, timeout: int) -> dict:
    """One HTTP response per base/date window, shared by all quote workers."""
    key = (base, start, end, timeout)
    with _BASE_RESPONSE_LOCK:
        future = _BASE_RESPONSES.get(key)
        owner = future is None
        if owner:
            # Bound retained full-history JSON. In-flight futures cannot be
            # evicted or sibling quote workers could duplicate their request.
            for old_key in list(_BASE_RESPONSES):
                if len(_BASE_RESPONSES) < _BASE_RESPONSE_CACHE_LIMIT:
                    break
                if _BASE_RESPONSES[old_key].done():
                    del _BASE_RESPONSES[old_key]
            future = Future()
            _BASE_RESPONSES[key] = future
    if owner:
        try:
            future.set_result(_get_json(f"{API_BASE}/{start}..{end}?from={base}", timeout))
        except Exception as exc:
            future.set_exception(exc)
    return future.result()


def _pivot_source_query(start: str, end: str) -> dict:
    return {"head_coverage_version": HEAD_COVERAGE_VERSION, "provider": "ECB",
            "url": f"{API_BASE}/{start}..{end}?from=EUR", "start": start, "end": end}


def _pivot_source_path(directory: Path, start: str, end: str) -> Path:
    query = json.dumps(_pivot_source_query(start, end), sort_keys=True).encode()
    return directory / ".head_sources" / f"{sha256_bytes(query)}.json"


def _validate_pivot_window(payload: dict, start: str, end: str) -> dict:
    # A 404 is not absence evidence. Require a nonempty, complete, correctly
    # scoped native ECB response; conservatively reject shifted boundaries.
    if (not isinstance(payload, dict) or payload.get("base") != "EUR"
            or payload.get("amount") != 1 or isinstance(payload.get("amount"), bool)
            or payload.get("start_date") != start or payload.get("end_date") != end):
        raise HistoricalSourceError("ECB pivot evidence has wrong base, amount or window")
    rates = payload.get("rates")
    if (not isinstance(rates, dict) or not rates or any(not isinstance(day, str) for day in rates)
            or min(rates) != start or max(rates) != end):
        raise HistoricalSourceError("ECB pivot evidence does not cover the exact requested window")
    for day, items in rates.items():
        try:
            parsed = datetime.strptime(day, "%Y-%m-%d").date().isoformat()
        except (TypeError, ValueError) as exc:
            raise HistoricalSourceError("ECB pivot evidence has invalid observation dates") from exc
        if parsed != day or not start <= day <= end or not isinstance(items, dict) or not items:
            raise HistoricalSourceError("ECB pivot evidence has invalid observation rows")
        for currency, value in items.items():
            if (not isinstance(currency, str) or len(currency) != 3 or not currency.isascii()
                    or not currency.isalpha() or currency != currency.upper()
                    or isinstance(value, bool) or not isinstance(value, (float, int))
                    or not math.isfinite(value) or value <= 0):
                raise HistoricalSourceError("ECB pivot evidence has invalid currency rates")
    return rates


def _verified_base_absence(base: str, start: str, end: str, directory: Path, timeout: int) -> dict:
    """Keep one source response per window; never derive/rebase price rows."""
    if base == "EUR":
        raise HistoricalSourceError("ECB native base cannot be treated as unpublished")
    source_path = _pivot_source_path(directory, start, end)
    query = _pivot_source_query(start, end)
    with _HEAD_SOURCE_LOCK:
        if source_path.exists():
            source = _read_pivot_source(source_path)
        else:
            source = None
    if source is None:
        payload = _base_rates("EUR", start, end, timeout)
        _validate_pivot_window(payload, start, end)
        candidate = {"query": query, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                     "response": payload}
        with _HEAD_SOURCE_LOCK:
            # Sibling quote workers share both the network future and this
            # immutable source evidence. Do not overwrite prior source bytes.
            if source_path.exists():
                source = _read_pivot_source(source_path)
            else:
                atomic_write_json(source_path, candidate)
                source = candidate
    if not isinstance(source, dict) or source.get("query") != query:
        raise HistoricalSourceError("ECB pivot evidence query contract mismatch")
    rates = _validate_pivot_window(source.get("response"), start, end)
    if any(base in items for items in rates.values()):
        raise HistoricalSourceError("ECB pivot contains base observations; original 404 remains a failure")
    return {"head_status": "verified_base_unpublished", "unpublished_base": base,
            "pivot_source_sha256": sha256_file(source_path), "pivot_observation_days": len(rates)}


def _read_pivot_source(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise HistoricalSourceError("existing ECB pivot evidence cannot be read; preserved unchanged") from exc


def _first_observed_date(path: Path) -> str:
    dates = _normalize_date_frame(_read_date_column(path))
    if dates.is_empty():
        raise ValueError("existing FX file has no valid dates")
    return dates.select(pl.col("date").min()).item().date().isoformat()


def _repair_history_head(record: SymbolRecord, start: str, end: str, path: Path, timeout: int) -> None:
    """Changing --start-date must actually repair the front, not only the tail."""
    proof_path = path.with_suffix(".head.json")
    try:
        proof = json.loads(proof_path.read_text())
    except (OSError, ValueError):
        proof = {}
    bound = (isinstance(proof, dict) and proof.get("requested_start", "9999") <= start
             and proof.get("source_sha256") == sha256_file(path))
    first_value = proof.get("first_observed_date") if bound else None
    first = datetime.strptime(first_value or _first_observed_date(path), "%Y-%m-%d").date()
    through = min(end, (first - timedelta(days=1)).isoformat())
    if bound and proof.get("checked_through", "") >= through:
        if proof.get("head_status") != "verified_base_unpublished":
            return
        pivot_path = _pivot_source_path(path.parent, proof["requested_start"], proof["checked_through"])
        if (proof.get("head_coverage_version") == HEAD_COVERAGE_VERSION
                and proof.get("unpublished_base") == record.base and pivot_path.is_file()
                and proof.get("pivot_source_sha256") == sha256_file(pivot_path)):
            return
    evidence = {"head_status": "not_required"}
    if start <= through:
        try:
            payload = _base_rates(record.base, start, through, timeout)
        except requests.HTTPError as exc:
            if getattr(exc.response, "status_code", None) != 404:
                raise
            evidence = _verified_base_absence(record.base, start, through, path.parent, timeout)
            payload = {"rates": {}}
        rates = payload.get("rates")
        if not isinstance(rates, dict) or (not rates and evidence["head_status"] != "verified_base_unpublished"):
            raise HistoricalSourceError("invalid ECB historical response")
        if evidence["head_status"] == "not_required":
            evidence["head_status"] = "source_queried"
        rows = []
        for day, items in rates.items():
            if not isinstance(items, dict):
                raise HistoricalSourceError("invalid ECB historical observation row")
            value = items.get(record.quote)
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, (float, int)):
                    raise HistoricalSourceError("invalid ECB historical rate type")
                number = float(value)
                if not math.isfinite(number) or number <= 0:
                    raise HistoricalSourceError("nonpositive or nonfinite FX rate")
                rows.append({"date": day, "open": number, "max": number, "min": number,
                             "close": number, "adjclose": number, "Trading_Volume": None})
        head = _normalize_rate_rows(rows, start, through)
        if not head.is_empty():
            old = _normalize_date_frame(_read_parquet(path))
            merged = pl.concat([head, old], how="diagonal_relaxed").unique(subset=["date"], keep="last").sort("date")
            _write_parquet(merged, path)
            first = merged.select(pl.col("date").min()).item().date()
    atomic_write_json(proof_path, {"provider": "ECB", "head_coverage_version": HEAD_COVERAGE_VERSION,
                                  "requested_start": start, **evidence,
                                  "first_observed_date": first.isoformat(),
                                  "checked_through": through, "source_sha256": sha256_file(path),
                                  "coverage_basis": "queried source; non-publication days are not fabricated"})


def _advance_head_proof_after_tail(path: Path, requested_start: str) -> None:
    proof_path = path.with_suffix(".head.json")
    try:
        proof = json.loads(proof_path.read_text())
    except (OSError, ValueError):
        proof = {"provider": "ECB", "requested_start": requested_start,
                 "coverage_basis": "original full-window source query"}
    proof["source_sha256"] = sha256_file(path)
    atomic_write_json(proof_path, proof)


def _resolve_api_end_date(timeout: int) -> str:
    payload = _get_json(f"{API_BASE}/latest", timeout)
    end_date = str(payload.get("date", "")).strip()
    if not end_date:
        raise RuntimeError("Frankfurter /latest did not include date")
    return end_date


def _load_default_pairs() -> list[str]:
    if not DEFAULT_SYMBOLS_PATH.exists():
        return [
            "EURUSD",
            "GBPUSD",
            "USDJPY",
            "AUDUSD",
            "USDCAD",
            "USDCHF",
            "NZDUSD",
            "EURJPY",
            "EURGBP",
            "EURCHF",
            "EURAUD",
            "EURNZD",
            "EURCAD",
            "GBPJPY",
            "GBPCHF",
        ]

    frame = pl.read_csv(
        DEFAULT_SYMBOLS_PATH, infer_schema=False, ignore_errors=True
    ).fill_null("")
    if "code" not in frame.columns:
        return []

    pairs: list[str] = []
    for raw in frame["code"].to_list():
        code = str(raw).strip().upper()
        if len(code) == 6 and code.isalpha():
            pairs.append(code)
    return pairs


def _load_pairs_from_txt(file_path: Path) -> list[str]:
    pairs: list[str] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip().upper().replace("=X", "").replace("-", "")
            if not value or value.startswith("#"):
                continue
            if len(value) == 6 and value.isalpha():
                pairs.append(value)
    return pairs


def _build_symbol_records(pairs: list[str], supported: set[str]) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    seen: set[str] = set()
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        base, quote = pair[:3], pair[3:]
        if base not in supported or quote not in supported:
            continue
        records.append(
            SymbolRecord(code=pair, name=pair, market="forex", base=base, quote=quote)
        )
    return records


def _normalize_date_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    date_expr = (
        pl.col("date").str.to_datetime(strict=False).alias("date")
        if frame.schema.get("date") == pl.String
        else pl.col("date").cast(pl.Datetime("us"), strict=False).alias("date")
    )
    return frame.with_columns(date_expr).drop_nulls("date").sort("date")


def _max_frame_date(frame: pl.DataFrame) -> str | None:
    normalized = _normalize_date_frame(frame)
    if normalized.is_empty():
        return None
    latest = normalized.select(pl.col("date").max()).item()
    return latest.date().isoformat()


def _existing_row_count_and_latest_date(path: Path) -> tuple[int, str | None]:
    rows = _read_parquet_row_count(path)
    try:
        metadata = pq.read_metadata(path)
        schema = metadata.schema.to_arrow_schema()
        date_idx = schema.get_field_index("date")
        if date_idx >= 0:
            latest = None
            for row_group_idx in range(metadata.num_row_groups):
                stats = metadata.row_group(row_group_idx).column(date_idx).statistics
                if stats is None or not bool(getattr(stats, "has_min_max", False)):
                    continue
                value = stats.max
                if isinstance(value, bytes):
                    value = value.decode("utf-8", errors="ignore")
                parsed = (
                    value.date().isoformat()
                    if isinstance(value, datetime)
                    else str(value)[:10]
                )
                latest = parsed if latest is None else max(latest, parsed)
            if latest is not None:
                return rows, latest
    except Exception:
        pass
    return rows, _max_frame_date(_read_date_column(path))


def _normalize_rate_rows(
    rows: list[dict[str, object]], fetch_start_date: str, end_date: str
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    start_dt = datetime.strptime(fetch_start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    return (
        pl.DataFrame(rows)
        .with_columns(
            pl.col("date").str.to_datetime(strict=False).alias("date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).alias(column)
                for column in (
                    "open",
                    "max",
                    "min",
                    "close",
                    "adjclose",
                    "Trading_Volume",
                )
                if column in rows[0]
            ],
        )
        .filter(pl.col("date").is_between(start_dt, end_dt, closed="both"))
        .drop_nulls(["date", "close"])
        .sort("date")
        .unique(subset=["date"], keep="last", maintain_order=True)
        .sort("date")
    )


def _download_pair(
    record: SymbolRecord,
    start_date: str,
    end_date: str,
    output_dir: Path,
    timeout: int,
    refresh: bool,
    incremental: bool,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    existing_rows = 0
    has_existing = False
    fetch_start_date = start_date

    if output_path.exists() and incremental:
        try:
            _repair_history_head(record, start_date, end_date, output_path, timeout)
            existing_rows, latest_date = _existing_row_count_and_latest_date(
                output_path
            )
            has_existing = existing_rows > 0
            if latest_date is not None:
                next_date = (
                    (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1))
                    .date()
                    .isoformat()
                )
                fetch_start_date = max(start_date, next_date)
            if datetime.strptime(fetch_start_date, "%Y-%m-%d") > datetime.strptime(
                end_date, "%Y-%m-%d"
            ):
                return DownloadResult(
                    code=record.code,
                    status="up_to_date",
                    rows=existing_rows,
                    output_path=str(output_path),
                )
        except Exception as exc:
            source_failure = isinstance(exc, (requests.RequestException, HistoricalSourceError))
            if source_failure:
                try:
                    existing_rows = _read_parquet_row_count(output_path)
                except Exception:
                    source_failure = False
            return DownloadResult(
                code=record.code,
                status="failed_head_source" if source_failure else "failed_existing_read",
                rows=existing_rows if source_failure else 0,
                output_path=str(output_path),
                message=str(exc),
            )

    if output_path.exists() and not refresh and not incremental:
        try:
            rows = _read_parquet_row_count(output_path)
            return DownloadResult(
                code=record.code,
                status="skipped_existing",
                rows=int(rows),
                output_path=str(output_path),
            )
        except Exception as exc:
            return DownloadResult(
                code=record.code,
                status="failed_existing_read",
                rows=0,
                output_path=str(output_path),
                message=str(exc),
            )

    try:
        payload = _base_rates(record.base, fetch_start_date, end_date, timeout)
        rates = payload.get("rates", {})
        if not isinstance(rates, dict) or not rates:
            return DownloadResult(
                code=record.code,
                status="empty",
                rows=0,
                output_path=None,
                message="No rates returned",
            )

        rows: list[dict[str, object]] = []
        for d, item in rates.items():
            if not isinstance(item, dict):
                continue
            close_value = item.get(record.quote)
            if close_value is None:
                continue
            close_num = float(close_value)
            if not math.isfinite(close_num) or close_num <= 0:
                raise ValueError("nonpositive or nonfinite FX rate")
            rows.append(
                {
                    "date": d,
                    "open": close_num,
                    "max": close_num,
                    "min": close_num,
                    "close": close_num,
                    "adjclose": close_num,
                    "Trading_Volume": None,
                }
            )

        if not rows:
            return DownloadResult(
                code=record.code,
                status="empty",
                rows=0,
                output_path=None,
                message="No usable rate points",
            )

        frame = _normalize_rate_rows(rows, fetch_start_date, end_date)
        if frame.is_empty():
            return DownloadResult(
                code=record.code,
                status="empty",
                rows=0,
                output_path=None,
                message="No usable rate points after date filtering",
            )

        if incremental and has_existing:
            existing_frame = _read_parquet(output_path)
            merged = (
                pl.concat(
                    [_normalize_date_frame(existing_frame), frame],
                    how="diagonal_relaxed",
                )
                .sort("date")
                .unique(subset=["date"], keep="last", maintain_order=True)
                .sort("date")
            )
            _write_parquet(merged, output_path)
            _advance_head_proof_after_tail(output_path, start_date)
            return DownloadResult(
                code=record.code,
                status="updated_incremental",
                rows=int(merged.height),
                output_path=str(output_path),
            )

        _write_parquet(frame, output_path)
        _advance_head_proof_after_tail(output_path, start_date)
        return DownloadResult(
            code=record.code,
            status="updated",
            rows=int(frame.height),
            output_path=str(output_path),
        )
    except Exception as exc:
        return DownloadResult(
            code=record.code,
            status="failed",
            rows=0,
            output_path=None,
            message=str(exc),
        )


def _run_download(args: argparse.Namespace) -> None:
    global _MAX_RETRIES, _RATE_LIMITER, _RETRY_BASE
    _BASE_RESPONSES.clear()
    _MAX_RETRIES = max(0, int(args.max_retries))
    _RETRY_BASE = max(0.1, float(args.retry_base))
    incremental_mode = args.incremental or args.mode == "daily-update"
    request_interval = resolve_request_interval(
        "frankfurter_public", args.request_interval
    )
    _RATE_LIMITER = SharedRateLimiter(request_interval, name="frankfurter_public")
    print(
        f"[frankfurter] {describe_rate_limit('frankfurter_public', request_interval)}",
        flush=True,
    )

    if args.refresh and incremental_mode:
        raise RuntimeError(
            "--refresh cannot be combined with daily incremental mode (--mode daily-update or --incremental)"
        )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    supported = _load_supported_currencies(args.timeout)
    api_latest = _resolve_api_end_date(args.timeout)
    requested_end = resolve_end_date(str(args.end_date))
    applied_end = min(requested_end, api_latest)

    if args.symbols_file:
        pairs = _load_pairs_from_txt(Path(args.symbols_file))
    else:
        pairs = _load_default_pairs()

    records = _build_symbol_records(pairs, supported)
    if not records:
        raise RuntimeError("No valid forex pairs resolved for Frankfurter")

    if not args.skip_manifest:
        _write_csv(
            pl.DataFrame([asdict(item) for item in records]),
            output_dir / "symbols.csv",
        )

    def _worker(record: SymbolRecord) -> DownloadResult:
        return _download_pair(
            record,
            args.start_date,
            applied_end,
            output_dir,
            args.timeout,
            args.refresh,
            incremental_mode,
        )

    results = run_parallel_tasks(
        records,
        _worker,
        max_workers=args.workers,
        desc="download:forex:frankfurter",
        unit="symbol",
    )

    results.sort(key=lambda item: item.code)

    report_columns = ["code", "status", "rows", "output_path", "message"]
    report_rows = [asdict(item) for item in results]
    report_frame = (
        pl.DataFrame(report_rows, infer_schema_length=None).select(report_columns)
        if report_rows
        else pl.DataFrame({column: [] for column in report_columns})
    )
    _write_csv(report_frame, output_dir / "download_report.csv")

    status_counts: dict[str, int] = {}
    row_count = 0
    for item in results:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
        row_count += int(item.rows)

    summary = {
        "provider": "frankfurter",
        "mode": "daily-update" if incremental_mode else "full",
        "requested_start_date": args.start_date,
        "requested_end_date": requested_end,
        "provider_end_date": api_latest,
        "applied_end_date": applied_end,
        "symbol_count": len(records),
        "row_count": row_count,
        "status_counts": status_counts,
        "acquisition_contract": acquisition_contract(),
    }
    _write_text(
        output_dir / "download_summary.json",
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
    )
    _BASE_RESPONSES.clear()
    if args.official_history:
        from downloader.frankfurter_official_history import run_official_history
        run_official_history(output_dir / "official_v2", max_requests=args.official_history_max_requests)

    print(
        "[download] provider=frankfurter "
        f"start={args.start_date} requested_end={requested_end} "
        f"provider_latest={api_latest} applied_end={applied_end} symbols={len(records)}"
    )
    print(f"[download] completed status_counts={status_counts}")
    failed = sum(
        count for status, count in status_counts.items() if status.startswith("failed")
    )
    if failed:
        raise RuntimeError(f"Frankfurter download incomplete: {failed} pairs failed")


def main() -> None:
    args = parse_args()
    with exclusive_dataset_lock(Path(args.output_dir) / ".download.lock",
                                provider="frankfurter", timeout_seconds=args.lock_timeout_seconds):
        _run_download(args)


if __name__ == "__main__":
    main()
