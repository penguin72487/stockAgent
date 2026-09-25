from __future__ import annotations

import argparse
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Callable

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from common import PersistentProgress, atomic_write_text
from artifact_io import atomic_write_parquet, sha256_file
from feature_stage_timing import stage_latency_summary
from ohlcv_hot_tail import hot_tail_path, logical_mtime_ns, read_logical_parquet


LEGACY_CONTRACT_VERSION = 6
MIDNIGHT_EXECUTION_CONTRACT_VERSION = 7
FUNDING_CONTRACT_VERSION = 3
DECISION_CUTOFF_MINUTES_UTC = 0
DEFAULT_EXECUTION_MINUTES_UTC = 5
EXPECTED_MINUTE_ROWS = 1440
MODEL_LOOKBACK_DAYS = 32
MODEL_RAW_SESSION_WINDOW_DAYS = MODEL_LOOKBACK_DAYS + 1
CANONICAL_MINUTE_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"


@dataclass(slots=True)
class MaterializeResult:
    symbol: str
    status: str
    rows: int
    executable_return_rows: int
    incomplete_feature_sessions_retained: int
    execution_sessions_excluded: int
    funding_events: int
    output_path: str | None
    output_sha256: str | None
    message: str | None = None
    build_mode: str = "unknown"
    stage_elapsed_seconds_json: str = "{}"


def _run_symbol_jobs(
    records: list[dict[str, object]],
    work: Callable[[dict[str, object]], MaterializeResult],
    *,
    workers: int,
    progress: PersistentProgress,
) -> list[MaterializeResult]:
    """Retry only a source-version race, once, after the first wave drains.

    A raw-minute writer can atomically replace a source while one symbol is
    being read.  Re-running that symbol with a fresh identity is safe; treating
    an incomplete or changed read as success is not.  Draining the first wave
    before retry gives the writer time to finish without delaying healthy
    symbols or counting a failed attempt as completed progress.
    """

    completed: list[MaterializeResult] = []
    pending = records
    for attempt in range(2):
        retry: list[dict[str, object]] = []
        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
            futures = {executor.submit(work, record): record for record in pending}
            for future in as_completed(futures):
                record = futures[future]
                symbol = str(record["code"])
                try:
                    result = future.result()
                except Exception as exc:
                    if (
                        attempt == 0
                        and type(exc) is RuntimeError
                        and str(exc)
                        == "Bybit source changed during daily materialization"
                    ):
                        retry.append(record)
                        continue
                    result = MaterializeResult(
                        symbol, "failed", 0, 0, 0, 0, 0, None, None,
                        f"{type(exc).__name__}: {exc}",
                    )
                completed.append(result)
                progress.update("materialize", result.status)
        if not retry:
            break
        progress.heartbeat("retry_source_changed")
        pending = retry
    return sorted(completed, key=lambda item: item.symbol)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize midnight-cutoff Bybit perpetual daily features and "
            "00:00 or legacy 00:05 UTC executions with "
            "official funding cash flows embedded in a total-return adjclose."
        )
    )
    parser.add_argument("--input-dir", default="data_bybit/1m")
    parser.add_argument("--funding-dir", default="data_bybit/funding")
    parser.add_argument("--output-dir", default="data_bybit/perpetual_daily")
    parser.add_argument(
        "--workers", type=int, default=max(1, min(12, os.cpu_count() or 1))
    )
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--execution-minute-utc",
        type=int,
        choices=(0, 5),
        default=DEFAULT_EXECUTION_MINUTES_UTC,
        help=(
            "minute after 00:00 UTC used for the executor-only Kline open; "
            "0 is the zero-latency research contract and 5 preserves v6"
        ),
    )
    return parser.parse_args()


def _contract_version(execution_minutes_utc: int) -> int:
    if execution_minutes_utc == 0:
        return MIDNIGHT_EXECUTION_CONTRACT_VERSION
    if execution_minutes_utc == 5:
        return LEGACY_CONTRACT_VERSION
    raise ValueError("execution_minutes_utc must be 0 or 5")


def _clock_hhmm(execution_minutes_utc: int) -> str:
    _contract_version(execution_minutes_utc)
    return f"00:{execution_minutes_utc:02d}"


def _sha256(path: Path) -> str:
    return sha256_file(path)


def _write_parquet_atomic(
    frame: pl.DataFrame,
    path: Path,
    *,
    before_replace: Callable[[], None] | None = None,
) -> None:
    atomic_write_parquet(
        path,
        frame,
        compression="snappy",
        write_statistics=True,
        before_replace=before_replace,
    )


def _parse_utc(column: str, schema: pl.Schema) -> pl.Expr:
    return (
        pl.col(column).str.to_datetime(strict=False, time_zone="UTC")
        if schema[column] == pl.String
        else pl.col(column).cast(pl.Datetime("us", "UTC"), strict=False)
    )


def _receipt_utc(value: object) -> datetime:
    resolved = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    return resolved.astimezone(timezone.utc)


def _standard_instruments(path: Path) -> pl.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            f"missing current Bybit instrument snapshot: {path}; run funding downloader first"
        )
    frame = pl.read_csv(path, infer_schema_length=10_000)
    required = {
        "code",
        "category",
        "quote_coin",
        "settle_coin",
        "contract_type",
        "status",
        "symbol_type",
        "is_pre_listing",
        "funding_interval_minutes",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"instrument snapshot missing columns: {sorted(missing)}")
    return frame.filter(
        (pl.col("category") == "linear")
        & (pl.col("quote_coin") == "USDT")
        & (pl.col("settle_coin") == "USDT")
        & pl.col("contract_type").cast(pl.String).str.contains("LinearPerpetual")
        & (pl.col("status") == "Trading")
        & pl.col("symbol_type").fill_null("").cast(pl.String).str.strip_chars().eq("")
        & ~pl.col("is_pre_listing").cast(pl.Boolean, strict=False).fill_null(False)
    ).sort("code")


def _daily_bars(
    source_path: Path,
    *,
    execution_minutes_utc: int = DEFAULT_EXECUTION_MINUTES_UTC,
    read_from_utc: datetime | None = None,
    audit: dict[str, object] | None = None,
) -> tuple[pl.DataFrame, int, int]:
    _contract_version(execution_minutes_utc)
    schema = pq.read_schema(source_path)
    required = {"date", "open", "max", "min", "close", "Trading_Volume"}
    missing = required - set(schema.names)
    if missing:
        raise ValueError(f"one-minute source missing columns: {sorted(missing)}")
    columns = [
        name
        for name in (
            "date",
            "open",
            "max",
            "min",
            "close",
            "Trading_Volume",
            "bybit_turnover",
        )
        if name in schema.names
    ]
    filters = None
    if read_from_utc is not None:
        if read_from_utc.tzinfo is None or read_from_utc.utcoffset() != timedelta(0):
            raise ValueError("read_from_utc must be timezone-aware UTC")
        date_type = schema.field("date").type
        if not (pa.types.is_string(date_type) or pa.types.is_large_string(date_type)):
            raise ValueError("bounded Bybit read requires canonical string timestamps")
        filters = [
            ("date", ">=", read_from_utc.strftime("%Y-%m-%d %H:%M:%S"))
        ]
    raw = read_logical_parquet(source_path, columns=columns, filters=filters)
    if audit is not None:
        audit["canonical_string_dates"] = bool(
            raw.schema.get("date") == pl.String
            and raw.select(
                pl.col("date")
                .str.contains(CANONICAL_MINUTE_DATE_PATTERN)
                .fill_null(False)
                .all()
            ).item()
        )
    timestamp = _parse_utc("date", raw.schema)
    normalized = (
        raw.with_columns(timestamp.alias("__ts"))
        .drop_nulls(["__ts", "open", "max", "min", "close"])
        .sort("__ts")
    )
    if normalized.select(pl.col("__ts").is_duplicated().any()).item():
        raise ValueError("duplicate one-minute timestamps")
    if normalized.select(
        (
            (pl.col("__ts").dt.second() != 0) | (pl.col("__ts").dt.microsecond() != 0)
        ).any()
    ).item():
        raise ValueError("one-minute timestamps are off grid")
    normalized = normalized.with_columns(
        (
            (
                pl.col("__ts") - pl.duration(minutes=DECISION_CUTOFF_MINUTES_UTC)
            ).dt.date()
            + pl.duration(days=1)
        ).alias("__session_end_date")
    )
    aggregations = [
        pl.col("open").first().cast(pl.Float64).alias("open"),
        pl.col("max").max().cast(pl.Float64).alias("max"),
        pl.col("min").min().cast(pl.Float64).alias("min"),
        pl.col("close").last().cast(pl.Float64).alias("close"),
        pl.col("Trading_Volume").sum().cast(pl.Float64).alias("Trading_Volume"),
        pl.len().cast(pl.Int32).alias("source_minute_rows"),
        pl.col("__ts").n_unique().cast(pl.Int32).alias("unique_minute_rows"),
        pl.col("__ts").first().alias("first_minute_utc"),
        pl.col("__ts").last().alias("last_minute_utc"),
    ]
    if "bybit_turnover" in normalized.columns:
        aggregations.append(
            pl.col("bybit_turnover").sum().cast(pl.Float64).alias("bybit_turnover")
        )
    grouped = (
        normalized.group_by("__session_end_date", maintain_order=True)
        .agg(aggregations)
        .with_columns(
            pl.col("__session_end_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("__decision_cutoff_utc")
        )
        .with_columns(
            (
                pl.col("__decision_cutoff_utc")
                + pl.duration(minutes=execution_minutes_utc)
            ).alias("__boundary_utc")
        )
        .join(
            normalized.select(
                pl.col("__ts").alias("__execution_time_utc"),
                pl.col("open").cast(pl.Float64).alias("execution_price"),
            ),
            left_on="__boundary_utc",
            right_on="__execution_time_utc",
            how="left",
        )
        .with_columns(
            (
                (pl.col("source_minute_rows") == EXPECTED_MINUTE_ROWS)
                & (pl.col("unique_minute_rows") == EXPECTED_MINUTE_ROWS)
                & pl.col("execution_price").is_finite()
                & (pl.col("execution_price") > 0.0)
                & (
                    pl.col("first_minute_utc")
                    == pl.col("__decision_cutoff_utc") - pl.duration(days=1)
                )
                & (
                    pl.col("last_minute_utc")
                    == pl.col("__decision_cutoff_utc") - pl.duration(minutes=1)
                )
            ).alias("minute_grid_complete")
        )
    )
    if "bybit_turnover" in grouped.columns:
        grouped = grouped.with_columns(
            pl.when(pl.col("execution_price") > 0.0)
            .then(pl.col("bybit_turnover") / pl.col("execution_price"))
            .otherwise(None)
            .alias("execution_volume_equivalent")
        )
    grouped = (
        grouped.sort("__decision_cutoff_utc")
        .with_columns(
            (pl.col("execution_price").is_finite() & (pl.col("execution_price") > 0.0))
            .fill_null(False)
            .alias("execution_available")
        )
        .with_columns(
            (
                (
                    pl.col("minute_grid_complete")
                    .cast(pl.Int16)
                    .rolling_sum(
                        window_size=MODEL_RAW_SESSION_WINDOW_DAYS,
                        min_samples=MODEL_RAW_SESSION_WINDOW_DAYS,
                    )
                    == MODEL_RAW_SESSION_WINDOW_DAYS
                )
                & (
                    pl.col("__decision_cutoff_utc")
                    - pl.col("__decision_cutoff_utc").shift(MODEL_LOOKBACK_DAYS)
                    == pl.duration(days=MODEL_LOOKBACK_DAYS)
                )
            )
            .fill_null(False)
            .alias("policy_tradable")
        )
    )
    incomplete_retained = int(
        grouped.select(
            ((~pl.col("minute_grid_complete")) & pl.col("execution_available")).sum()
        ).item()
    )
    execution_excluded = int(
        grouped.select((~pl.col("execution_available")).sum()).item()
    )
    if audit is not None:
        audit["execution_excluded_dates"] = (
            grouped.filter(~pl.col("execution_available"))
            .get_column("__session_end_date")
            .cast(pl.String)
            .to_list()
        )
    # A missing feature minute must never erase a real execution mark. Retain
    # every execution-valued row for recurrent valuation and funding accounting;
    # policy_tradable remains false until a complete causal 32-day feature
    # window (plus the predecessor required by delta features) is available.
    executable_marks = grouped.filter(pl.col("execution_available")).sort(
        "__boundary_utc"
    )
    return executable_marks, incomplete_retained, execution_excluded


def _file_identity(path: Path) -> dict[str, int] | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"materialization source is not a regular file: {path}")
    info = path.stat(follow_symlinks=False)
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
    }


def _tail_start_date(path: Path) -> str | None:
    if not path.is_file():
        return None
    parquet = pq.ParquetFile(path)
    try:
        index = parquet.schema.names.index("date")
    except ValueError as exc:
        raise ValueError("hot tail has no date column") from exc
    date_values = pl.from_arrow(parquet.read(columns=["date"]))
    if date_values.schema.get("date") != pl.String or not date_values.select(
        pl.col("date")
        .str.contains(CANONICAL_MINUTE_DATE_PATTERN)
        .fill_null(False)
        .all()
    ).item():
        raise ValueError("hot tail has noncanonical minute timestamps")
    starts: list[date] = []
    for group_index in range(parquet.metadata.num_row_groups):
        stats = parquet.metadata.row_group(group_index).column(index).statistics
        if stats is None or not stats.has_min_max:
            raise ValueError("hot tail has no complete date statistics")
        starts.append(date.fromisoformat(str(stats.min)[:10]))
    if not starts:
        raise ValueError("hot tail has no row groups")
    return min(starts).isoformat()


def _daily_from_verified_output(path: Path, *, contract_version: int) -> pl.DataFrame:
    prior = pl.read_parquet(path)
    required = {
        "date", "execution_price", "source_minute_rows", "unique_minute_rows",
        "minute_grid_complete", "policy_tradable", "execution_available",
        "first_minute_utc", "last_minute_utc", "bybit_perpetual_contract_version",
    }
    if required - set(prior.columns):
        raise ValueError("prior daily output lacks incremental input columns")
    versions = prior.get_column("bybit_perpetual_contract_version")
    if prior.is_empty() or versions.null_count() or versions.n_unique() != 1 or int(versions[0]) != contract_version:
        raise ValueError("prior daily output uses a different execution contract")
    dates = prior.get_column("date")
    if dates.null_count() or dates.n_unique() != prior.height or not dates.is_sorted():
        raise ValueError("prior daily dates are not unique and ordered")
    return (
        prior.with_columns(pl.col("date").str.to_date(strict=True).alias("__session_end_date"))
        .with_columns(
            pl.col("__session_end_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("__decision_cutoff_utc")
        )
        .with_columns(
            (
                pl.col("__decision_cutoff_utc")
                + pl.duration(minutes=DEFAULT_EXECUTION_MINUTES_UTC if contract_version == LEGACY_CONTRACT_VERSION else 0)
            ).alias("__boundary_utc")
        )
    )


def _incremental_daily_bars(
    source: Path,
    target: Path,
    sidecar: Path,
    *,
    execution_minutes_utc: int,
) -> tuple[pl.DataFrame, int, int, list[str]] | None:
    if not sidecar.is_file() or not target.is_file():
        return None
    try:
        proof = json.loads(sidecar.read_text(encoding="utf-8"))
        if (
            proof.get("schema_version") != 1
            or proof.get("execution_contract_version") != _contract_version(execution_minutes_utc)
            or proof.get("canonical_string_dates") is not True
            or proof.get("base_identity") != _file_identity(source)
            or proof.get("output_sha256") != _sha256(target)
        ):
            return None
        prior = _daily_from_verified_output(
            target, contract_version=_contract_version(execution_minutes_utc)
        )
        old_tail_start = proof.get("tail_start_date")
        new_tail_start = _tail_start_date(hot_tail_path(source))
        earliest = min(
            (date.fromisoformat(value) for value in (old_tail_start, new_tail_start) if value),
            default=None,
        )
        excluded_before = proof["execution_excluded_dates"]
        if not isinstance(excluded_before, list) or not all(isinstance(value, str) for value in excluded_before):
            return None
        if earliest is None:
            daily = prior
            excluded_dates = excluded_before
        else:
            read_from = datetime.combine(
                earliest - timedelta(days=MODEL_RAW_SESSION_WINDOW_DAYS),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            tail_audit: dict[str, object] = {}
            suffix, _, _ = _daily_bars(
                source,
                execution_minutes_utc=execution_minutes_utc,
                read_from_utc=read_from,
                audit=tail_audit,
            )
            if tail_audit.get("canonical_string_dates") is not True:
                return None
            daily = pl.concat(
                [
                    prior.filter(pl.col("__session_end_date") < earliest),
                    suffix.filter(pl.col("__session_end_date") >= earliest),
                ],
                how="diagonal_relaxed",
            ).sort("__decision_cutoff_utc")
            excluded_dates = [value for value in excluded_before if value < earliest.isoformat()]
            excluded_dates.extend(
                value
                for value in tail_audit["execution_excluded_dates"]
                if value >= earliest.isoformat()
            )
        incomplete = int(
            daily.select(
                ((~pl.col("minute_grid_complete")) & pl.col("execution_available")).sum()
            ).item()
        )
        return daily, incomplete, len(excluded_dates), excluded_dates
    except (KeyError, OSError, TypeError, ValueError, pl.exceptions.PolarsError):
        return None


def _attach_funding_total_return(
    daily: pl.DataFrame,
    funding_path: Path,
    coverage_row: dict[str, object],
    *,
    execution_minutes_utc: int = DEFAULT_EXECUTION_MINUTES_UTC,
) -> tuple[pl.DataFrame, int, int]:
    contract_version = _contract_version(execution_minutes_utc)
    execution_clock = _clock_hhmm(execution_minutes_utc)
    if "execution_available" not in daily.columns:
        daily = daily.with_columns(
            (pl.col("execution_price").is_finite() & (pl.col("execution_price") > 0.0))
            .fill_null(False)
            .alias("execution_available")
        )
    if "policy_tradable" not in daily.columns:
        daily = daily.with_columns(
            pl.col("minute_grid_complete")
            .cast(pl.Boolean, strict=False)
            .fill_null(False)
            .alias("policy_tradable")
        )
    head_complete_value = str(coverage_row.get("head_complete", "")).strip().lower()
    if head_complete_value not in {"1", "true", "yes"}:
        raise ValueError("funding coverage is not head-complete")
    funding = pl.read_parquet(funding_path)
    required = {
        "funding_time_utc",
        "funding_rate",
        "funding_mark_price",
        "bybit_funding_contract_version",
    }
    missing = required - set(funding.columns)
    if missing:
        raise ValueError(f"funding parquet missing columns: {sorted(missing)}")
    if (
        funding.height
        and int(funding["bybit_funding_contract_version"].max())
        != FUNDING_CONTRACT_VERSION
    ):
        raise ValueError(
            "funding parquet uses an incompatible launch-boundary/mark-price contract"
        )
    coverage_start = _receipt_utc(coverage_row["coverage_start_utc"])
    coverage_end = _receipt_utc(coverage_row["coverage_end_utc"])
    funding_times = (
        funding.with_columns(
            _parse_utc("funding_time_utc", funding.schema).alias("__ts")
        )
        .drop_nulls(["__ts", "funding_rate", "funding_mark_price"])
        .sort("__ts")
    )
    event_ts = funding_times["__ts"].to_list()
    event_rates = funding_times["funding_rate"].cast(pl.Float64).to_numpy()
    event_marks = funding_times["funding_mark_price"].cast(pl.Float64).to_numpy()

    boundaries = daily["__boundary_utc"].to_list()
    decision_cutoffs = daily["__decision_cutoff_utc"].to_list()
    execution_prices = daily["execution_price"].cast(pl.Float64).to_numpy()
    row_count = daily.height
    adjclose = np.asarray(execution_prices, dtype=np.float64).copy()
    event_counts = np.zeros(row_count, dtype=np.int32)
    funding_coefficients = np.full(row_count, np.nan, dtype=np.float64)
    funding_rate_sums = np.full(row_count, np.nan, dtype=np.float64)
    funding_last_rates = np.full(row_count, np.nan, dtype=np.float64)
    funding_last_event_times = np.full(
        row_count, np.datetime64("NaT", "us"), dtype="datetime64[us]"
    )
    price_returns = np.full(row_count, np.nan, dtype=np.float64)
    effective_returns = np.full(row_count, np.nan, dtype=np.float64)
    quarantined = np.ones(row_count, dtype=bool)
    executable = 0
    for row in range(max(0, row_count - 1)):
        start = boundaries[row]
        end = boundaries[row + 1]
        consecutive = end - start == timedelta(days=1)
        covered = start >= coverage_start and end <= coverage_end
        if (
            not consecutive
            or not covered
            or execution_prices[row] <= 0.0
            or execution_prices[row + 1] <= 0.0
        ):
            adjclose[row + 1] = execution_prices[row + 1]
            continue
        left = bisect_right(event_ts, start)
        right = bisect_right(event_ts, end)
        coefficient = 0.0
        rate_sum = 0.0
        last_rate = np.nan
        last_event_time = np.datetime64("NaT", "us")
        for cursor in range(left, right):
            coefficient += (
                float(event_marks[cursor]) / float(execution_prices[row])
            ) * float(event_rates[cursor])
            rate_sum += float(event_rates[cursor])
            last_rate = float(event_rates[cursor])
            last_event_time = np.datetime64(event_ts[cursor].replace(tzinfo=None), "us")
        count = right - left
        price_return = (
            float(execution_prices[row + 1]) / float(execution_prices[row]) - 1.0
        )
        effective_return = price_return - coefficient
        if not np.isfinite(effective_return) or 1.0 + effective_return <= 0.0:
            adjclose[row + 1] = execution_prices[row + 1]
            continue
        event_counts[row] = count
        funding_coefficients[row] = coefficient
        funding_rate_sums[row] = rate_sum
        funding_last_rates[row] = last_rate
        funding_last_event_times[row] = last_event_time
        price_returns[row] = price_return
        effective_returns[row] = effective_return
        quarantined[row] = False
        adjclose[row + 1] = adjclose[row] * (1.0 + effective_return)
        executable += 1

    previous_event_counts = np.full(row_count, np.nan, dtype=np.float64)
    previous_coefficients = np.full(row_count, np.nan, dtype=np.float64)
    previous_rate_sums = np.full(row_count, np.nan, dtype=np.float64)
    previous_last_rates = np.full(row_count, np.nan, dtype=np.float64)
    previous_last_event_times = np.full(
        row_count, np.datetime64("NaT", "us"), dtype="datetime64[us]"
    )
    # Model-side funding features end at the midnight decision cutoff, while
    # Model features always stop at the 00:00 decision cutoff. For the v7
    # zero-latency research contract, a funding event exactly on the boundary
    # settles before the new 00:00 target is valued: (start, end] belongs to the
    # position carried into end, while a newly opened position starts after it.
    for row in range(1, row_count):
        start = decision_cutoffs[row - 1]
        end = decision_cutoffs[row]
        if (
            end - start != timedelta(days=1)
            or start < coverage_start
            or end > coverage_end
            or execution_prices[row - 1] <= 0.0
        ):
            continue
        left = bisect_right(event_ts, start)
        right = bisect_right(event_ts, end)
        previous_event_counts[row] = right - left
        previous_rate_sums[row] = float(event_rates[left:right].sum())
        previous_coefficients[row] = float(
            (
                event_marks[left:right]
                / float(execution_prices[row - 1])
                * event_rates[left:right]
            ).sum()
        )
        if right > 0:
            previous_last_rates[row] = float(event_rates[right - 1])
            previous_last_event_times[row] = np.datetime64(
                event_ts[right - 1].replace(tzinfo=None), "us"
            )
    decision_cutoffs_us = np.asarray(
        [
            value.astimezone(timezone.utc).replace(tzinfo=None)
            if value.tzinfo is not None
            else value
            for value in decision_cutoffs
        ],
        dtype="datetime64[us]",
    )
    previous_funding_age_hours = (
        decision_cutoffs_us - previous_last_event_times
    ).astype("timedelta64[s]").astype(np.float64) / 3600.0
    previous_funding_age_hours[np.isnat(previous_last_event_times)] = np.nan

    result = daily.with_columns(
        pl.Series("adjclose", adjclose),
        pl.Series("funding_event_count_to_next", event_counts),
        pl.Series("funding_cashflow_coefficient_to_next", funding_coefficients),
        pl.Series("funding_rate_sum_to_next", funding_rate_sums),
        pl.Series("funding_last_rate_to_next", funding_last_rates),
        pl.Series("price_simple_return_to_next", price_returns),
        pl.Series("funding_adjusted_simple_return_to_next", effective_returns),
        pl.Series("funding_event_count_previous_session", previous_event_counts),
        pl.Series(
            "funding_cashflow_coefficient_previous_session", previous_coefficients
        ),
        pl.Series("funding_rate_sum_previous_session", previous_rate_sums),
        pl.Series("funding_last_rate_previous_session", previous_last_rates),
        pl.Series("funding_age_hours_at_decision", previous_funding_age_hours),
        pl.Series("return_quarantined", quarantined),
        pl.lit(contract_version, dtype=pl.Int16).alias(
            "bybit_perpetual_contract_version"
        ),
        pl.lit("00:00", dtype=pl.String).alias("decision_cutoff_utc"),
        pl.lit(execution_clock, dtype=pl.String).alias("daily_boundary_utc"),
    ).with_columns(pl.col("__session_end_date").cast(pl.String).alias("date"))
    output_columns = [
        "date",
        "open",
        "max",
        "min",
        "close",
        "execution_price",
        "adjclose",
        "Trading_Volume",
    ]
    if "bybit_turnover" in result.columns:
        output_columns.append("bybit_turnover")
    if "execution_volume_equivalent" in result.columns:
        output_columns.append("execution_volume_equivalent")
    output_columns.extend(
        [
            "source_minute_rows",
            "unique_minute_rows",
            "minute_grid_complete",
            "policy_tradable",
            "execution_available",
            "first_minute_utc",
            "last_minute_utc",
            "funding_event_count_to_next",
            "funding_cashflow_coefficient_to_next",
            "funding_rate_sum_to_next",
            "funding_last_rate_to_next",
            "price_simple_return_to_next",
            "funding_adjusted_simple_return_to_next",
            "funding_event_count_previous_session",
            "funding_cashflow_coefficient_previous_session",
            "funding_rate_sum_previous_session",
            "funding_last_rate_previous_session",
            "funding_age_hours_at_decision",
            "return_quarantined",
            "bybit_perpetual_contract_version",
            "decision_cutoff_utc",
            "daily_boundary_utc",
        ]
    )
    return result.select(output_columns), executable, int(event_counts.sum())


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc)
    input_dir = Path(args.input_dir)
    funding_dir = Path(args.funding_dir)
    output_dir = Path(args.output_dir)
    execution_minutes_utc = int(args.execution_minute_utc)
    contract_version = _contract_version(execution_minutes_utc)
    execution_clock = _clock_hhmm(execution_minutes_utc)
    output_dir.mkdir(parents=True, exist_ok=True)
    instruments_path = funding_dir / "instruments.csv"
    coverage_path = funding_dir / "funding_coverage.csv"
    receipt_identity_before = (
        _file_identity(instruments_path), _file_identity(coverage_path)
    )
    instruments = _standard_instruments(instruments_path)
    requested = {
        str(value).strip().upper()
        for value in (args.symbols or [])
        if str(value).strip()
    }
    if requested:
        instruments = instruments.filter(
            pl.col("code").str.to_uppercase().is_in(requested)
        )
        missing = requested - set(instruments["code"].str.to_uppercase().to_list())
        if missing:
            raise ValueError(
                f"symbols outside standard linear-USDT universe: {sorted(missing)}"
            )
    if args.limit is not None:
        instruments = instruments.head(max(0, int(args.limit)))
    if not coverage_path.is_file():
        raise FileNotFoundError(f"missing funding coverage receipt: {coverage_path}")
    coverage = {
        str(row["symbol"]): row
        for row in pl.read_csv(coverage_path, infer_schema_length=10_000).to_dicts()
    }
    if receipt_identity_before != (
        _file_identity(instruments_path), _file_identity(coverage_path)
    ):
        raise RuntimeError("Bybit funding receipt changed while loading symbol coverage")
    records = instruments.to_dicts()
    progress = PersistentProgress(
        output_dir / "progress.json",
        label=f"Bybit 永續日頻 00:00 決策 / {execution_clock} 執行",
        total=len(records),
        unit="symbol",
        basis="completed local 1m plus official funding materializations",
        started_at=started,
    )
    def work(record: dict[str, object]) -> MaterializeResult:
        work_started = time.perf_counter()
        symbol = str(record["code"])
        source = input_dir / f"{symbol}_features.parquet"
        funding_path = funding_dir / f"{symbol}_funding.parquet"
        target = output_dir / f"{symbol}_features.parquet"
        sidecar = output_dir / f"{symbol}_features.materialize.json"
        if not source.is_file():
            raise FileNotFoundError(f"missing one-minute source: {source}")
        if not funding_path.is_file() or symbol not in coverage:
            raise FileNotFoundError(
                f"missing complete funding source/receipt for {symbol}"
            )
        if (
            not args.refresh
            and target.is_file()
            and target.stat().st_mtime_ns
            >= max(logical_mtime_ns(source), funding_path.stat().st_mtime_ns)
            and "bybit_perpetual_contract_version" in pq.read_schema(target).names
            and "execution_volume_equivalent" in pq.read_schema(target).names
            and int(
                pl.read_parquet(target, columns=["bybit_perpetual_contract_version"])[
                    "bybit_perpetual_contract_version"
                ].max()
            )
            == contract_version
        ):
            frame = pl.read_parquet(target)
            target_sha256 = _sha256(target)
            elapsed = round(time.perf_counter() - work_started, 6)
            return MaterializeResult(
                symbol,
                "skipped_up_to_date",
                frame.height,
                int((~frame["return_quarantined"]).sum()),
                0,
                0,
                int(frame["funding_event_count_to_next"].sum()),
                str(target),
                target_sha256,
                build_mode="skipped_up_to_date",
                stage_elapsed_seconds_json=json.dumps(
                    {"up_to_date_proof": elapsed, "total": elapsed},
                    sort_keys=True,
                ),
            )
        source_identity = (
            _file_identity(source),
            _file_identity(hot_tail_path(source)),
            _file_identity(funding_path),
            _file_identity(instruments_path),
            _file_identity(coverage_path),
        )
        incremental = (
            _incremental_daily_bars(
                source,
                target,
                sidecar,
                execution_minutes_utc=execution_minutes_utc,
            )
            if not args.refresh
            else None
        )
        if incremental is None:
            daily_audit: dict[str, object] = {}
            daily, incomplete_retained, execution_excluded = _daily_bars(
                source,
                execution_minutes_utc=execution_minutes_utc,
                audit=daily_audit,
            )
            excluded_dates = daily_audit["execution_excluded_dates"]
            canonical_string_dates = daily_audit["canonical_string_dates"]
            build_mode = "full"
        else:
            daily, incomplete_retained, execution_excluded, excluded_dates = incremental
            canonical_string_dates = True
            build_mode = "incremental"
        daily_done = time.perf_counter()
        output, executable, events = _attach_funding_total_return(
            daily,
            funding_path,
            coverage[symbol],
            execution_minutes_utc=execution_minutes_utc,
        )
        funding_done = time.perf_counter()
        def assert_sources_unchanged() -> None:
            if source_identity != (
                _file_identity(source),
                _file_identity(hot_tail_path(source)),
                _file_identity(funding_path),
                _file_identity(instruments_path),
                _file_identity(coverage_path),
            ):
                raise RuntimeError("Bybit source changed during daily materialization")

        _write_parquet_atomic(output, target, before_replace=assert_sources_unchanged)
        output_sha256 = _sha256(target)
        assert_sources_unchanged()
        try:
            tail_start_date = _tail_start_date(hot_tail_path(source))
        except ValueError:
            # A source without row-group statistics still has a valid full
            # materialization, but cannot certify a bounded future rebuild.
            tail_start_date = None
            incremental_proof_available = False
        else:
            incremental_proof_available = True
        if incremental_proof_available and canonical_string_dates:
            atomic_write_text(
                sidecar,
                json.dumps(
                    {
                        "schema_version": 1,
                        "execution_contract_version": contract_version,
                        "canonical_string_dates": True,
                        "base_identity": source_identity[0],
                        "tail_start_date": tail_start_date,
                        "execution_excluded_dates": excluded_dates,
                        "output_sha256": output_sha256,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ) + "\n",
            )
        work_done = time.perf_counter()
        return MaterializeResult(
            symbol,
            "updated",
            output.height,
            executable,
            incomplete_retained,
            execution_excluded,
            events,
            str(target),
            output_sha256,
            build_mode=build_mode,
            stage_elapsed_seconds_json=json.dumps(
                {
                    "daily_bars": round(daily_done - work_started, 6),
                    "funding_join": round(funding_done - daily_done, 6),
                    "write_and_proof": round(work_done - funding_done, 6),
                    "total": round(work_done - work_started, 6),
                },
                sort_keys=True,
            ),
        )

    ordered = _run_symbol_jobs(
        records, work, workers=args.workers, progress=progress
    )
    atomic_write_text(
        output_dir / "materialize_report.csv",
        pl.DataFrame(
            [asdict(item) for item in ordered], infer_schema_length=None
        ).write_csv(),
    )
    completed_symbols = {item.symbol for item in ordered if item.status != "failed"}
    selected_instruments = instruments.filter(pl.col("code").is_in(completed_symbols))
    atomic_write_text(output_dir / "symbols.csv", selected_instruments.write_csv())
    failed = [item for item in ordered if item.status == "failed"]
    summary = {
        "contract_version": contract_version,
        "product": "bybit_standard_linear_usdt_perpetual",
        "decision_cutoff_utc": "00:00",
        "execution_boundary_utc": execution_clock,
        "decision_to_execution_lag_minutes": execution_minutes_utc,
        "position_contract": "daily_rebalance_positions_may_carry",
        "feature_session_contract": "previous_calendar_day_0000_through_2359_utc",
        "execution_price_contract": (
            "first_official_1m_kline_open_at_00:00_utc_zero_latency_research_assumption"
            if execution_minutes_utc == 0
            else "first_official_1m_kline_open_at_00:05_utc_after_five_minute_decision_lag"
        ),
        "funding_contract": "official_event_rate_times_official_hourly_mark_open_over_execution_price",
        "symbols": len(records),
        "completed_symbols": len(records) - len(failed),
        "failed_symbols": len(failed),
        "build_mode_counts": {
            mode: sum(item.build_mode == mode for item in ordered)
            for mode in ("full", "incremental", "skipped_up_to_date", "unknown")
        },
        "stage_latency": stage_latency_summary(ordered),
        "rows": sum(item.rows for item in ordered),
        "executable_return_rows": sum(item.executable_return_rows for item in ordered),
        "incomplete_feature_sessions_retained": sum(
            item.incomplete_feature_sessions_retained for item in ordered
        ),
        "execution_sessions_excluded": sum(
            item.execution_sessions_excluded for item in ordered
        ),
        "funding_events": sum(item.funding_events for item in ordered),
        "started_at_utc": started.isoformat(),
        "ended_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    atomic_write_text(
        output_dir / "materialize_summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    progress.finish(failed=bool(failed), require_exact=True)
    print(json.dumps(summary, ensure_ascii=False))
    if failed:
        raise RuntimeError(f"daily materialization failed for {len(failed)} symbols")


if __name__ == "__main__":
    main()
