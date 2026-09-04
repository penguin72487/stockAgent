from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    SharedRateLimiter,
    provider_rate_limit,
    resolve_end_date,
    retry_delay_seconds,
)
from artifact_io import (  # noqa: E402
    atomic_write_bytes as _atomic_bytes,
    atomic_write_json as _atomic_json,
    atomic_write_parquet as _atomic_parquet,
    sha256_bytes as _sha256_bytes,
    sha256_file as _sha256_path,
)


BASE_URL = "https://community-api.coinmetrics.io/v4"
CATALOG_ENDPOINT = "/catalog-v2/asset-metrics"
METRICS_ENDPOINT = "/timeseries/asset-metrics"
FREQUENCY = "1d"
PAGE_SIZE = 10_000
SCHEMA_VERSION = 1
_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True, slots=True)
class AssetSpec:
    asset: str
    metrics: tuple[str, ...]
    catalog_start: str
    catalog_end: str
    query_start: str


@dataclass(slots=True)
class AssetResult:
    asset: str
    status: str
    metrics_registered: int
    metrics_with_data: int
    rows: int
    rows_added: int
    vintage_rows_added: int
    first_date: str | None
    last_date: str | None
    output_path: str
    vintage_path: str
    raw_sha256: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill every catalog-declared Coin Metrics Community daily metric "
            "with retrieval vintages and resumable per-asset parquet outputs."
        )
    )
    parser.add_argument("--output-dir", default="data_coinmetrics_community")
    parser.add_argument("--start-date", default="2009-01-01")
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-assets", type=int, default=50)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--assets", nargs="*", default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--max-retries", type=int, default=6)
    parser.add_argument("--retry-base", type=float, default=1.0)
    return parser.parse_args()


def _safe_component(value: str) -> str:
    text = str(value).strip()
    safe = _SAFE_COMPONENT_RE.sub("_", text).strip("._")
    if not safe or safe in {".", ".."}:
        safe = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return safe


def _metric_column(metric: str) -> str:
    return "coinmetrics_" + _SAFE_COMPONENT_RE.sub("_", metric).strip("_")


def _date_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.date().isoformat()


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""} or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


class CoinMetricsClient:
    def __init__(self, *, max_retries: int, retry_base: float) -> None:
        profile = provider_rate_limit("coinmetrics_community")
        self.limiter = SharedRateLimiter(
            profile.interval_seconds,
            name="coinmetrics_community",
        )
        self.max_retries = max(0, int(max_retries))
        self.retry_base = max(0.1, float(retry_base))

    def _get(self, url: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "User-Agent": "stockAgent/1",
                },
            )
            try:
                with urlopen(request, timeout=90) as response:
                    payload = json.load(response)
                if not isinstance(payload, dict):
                    raise RuntimeError("Coin Metrics response is not an object")
                return payload
            except HTTPError as exc:
                last_error = exc
                if (
                    exc.code in {408, 429, 500, 502, 503, 504}
                    and attempt < self.max_retries
                ):
                    self.limiter.defer(
                        retry_delay_seconds(
                            attempt,
                            base=self.retry_base,
                            retry_after=exc.headers.get("Retry-After"),
                        )
                    )
                    continue
                raise
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self.limiter.defer(
                        retry_delay_seconds(attempt, base=self.retry_base)
                    )
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("Coin Metrics request failed without an explicit error")

    def get_all(
        self, path: str, params: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], bytes, int]:
        url = f"{BASE_URL}{path}?{urlencode(params)}"
        rows: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        seen: set[str] = set()
        while url:
            if url in seen:
                raise RuntimeError(f"Coin Metrics pagination repeated URL for {path}")
            seen.add(url)
            payload = self._get(url)
            page_rows = payload.get("data") or []
            if not isinstance(page_rows, list):
                raise RuntimeError(f"Coin Metrics {path} data is not a list")
            rows.extend(row for row in page_rows if isinstance(row, dict))
            pages.append(payload)
            url = str(payload.get("next_page_url") or "")
        raw = json.dumps(
            {
                "path": path,
                "params": params,
                "page_count": len(pages),
                "data": rows,
                "pagination_complete": True,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return rows, raw, len(pages)


def _catalog_specs(
    client: CoinMetricsClient,
    *,
    requested_start: str,
    requested_end: str,
    selected_assets: set[str] | None,
    limit: int | None,
) -> tuple[list[AssetSpec], bytes]:
    rows, raw, _ = client.get_all(CATALOG_ENDPOINT, {})
    specs: list[AssetSpec] = []
    seen_columns: dict[str, str] = {}
    for item in rows:
        asset = str(item.get("asset") or "").strip()
        if not asset or (selected_assets is not None and asset not in selected_assets):
            continue
        metrics: dict[str, tuple[str, str]] = {}
        for metric_item in item.get("metrics", []):
            if not isinstance(metric_item, dict):
                continue
            metric = str(metric_item.get("metric") or "").strip()
            if not metric:
                continue
            column = _metric_column(metric)
            collision = seen_columns.get(column)
            if collision is not None and collision != metric:
                raise RuntimeError(
                    f"Coin Metrics column collision: {collision!r} and {metric!r} -> {column!r}"
                )
            seen_columns[column] = metric
            for frequency in metric_item.get("frequencies", []):
                if (
                    isinstance(frequency, dict)
                    and frequency.get("community") is True
                    and str(frequency.get("frequency")) == FREQUENCY
                ):
                    min_time = _date_text(frequency.get("min_time"))
                    max_time = _date_text(frequency.get("max_time"))
                    if min_time and max_time:
                        metrics[metric] = (min_time, max_time)
        if not metrics:
            continue
        catalog_start = max(
            requested_start, min(value[0] for value in metrics.values())
        )
        catalog_end = min(requested_end, max(value[1] for value in metrics.values()))
        if catalog_start > catalog_end:
            continue
        specs.append(
            AssetSpec(
                asset=asset,
                metrics=tuple(sorted(metrics)),
                catalog_start=catalog_start,
                catalog_end=catalog_end,
                query_start=catalog_start,
            )
        )
    specs.sort(key=lambda item: item.asset)
    if limit is not None:
        specs = specs[: max(0, int(limit))]
    return specs, raw


def _query_start_for_asset(
    spec: AssetSpec,
    output_path: Path,
    *,
    refresh: bool,
) -> str:
    if refresh or not output_path.is_file():
        return spec.catalog_start
    try:
        frame = pl.read_parquet(output_path)
    except Exception:
        return spec.catalog_start
    latest_dates: list[str] = []
    for metric in spec.metrics:
        column = _metric_column(metric)
        if column not in frame.columns:
            return spec.catalog_start
        selected = frame.filter(pl.col(column).is_not_null())
        if selected.is_empty():
            return spec.catalog_start
        latest = selected["date"].max()
        if latest:
            latest_dates.append(str(latest))
    if len(latest_dates) != len(spec.metrics):
        return spec.catalog_start
    overlap = date.fromisoformat(min(latest_dates)) - timedelta(days=1)
    return max(spec.catalog_start, overlap.isoformat())


def _batched(values: list[AssetSpec], size: int) -> list[list[AssetSpec]]:
    chunk = max(1, int(size))
    return [values[index : index + chunk] for index in range(0, len(values), chunk)]


def _build_batches(
    specs: list[AssetSpec],
    output_dir: Path,
    *,
    refresh: bool,
    batch_assets: int,
) -> list[list[AssetSpec]]:
    grouped: dict[tuple[str, ...], list[AssetSpec]] = defaultdict(list)
    assets_dir = output_dir / "assets"
    for spec in specs:
        path = assets_dir / f"{_safe_component(spec.asset)}_features.parquet"
        query_start = _query_start_for_asset(spec, path, refresh=refresh)
        updated = AssetSpec(
            asset=spec.asset,
            metrics=spec.metrics,
            catalog_start=spec.catalog_start,
            catalog_end=spec.catalog_end,
            query_start=query_start,
        )
        # Assets with the same metric signature can share one official API
        # request even when their listing/last-local dates differ. Querying
        # from the earliest required date yields no rows before a later asset's
        # own coverage, but avoids thousands of one-asset requests.
        grouped[spec.metrics].append(updated)
    batches: list[list[AssetSpec]] = []
    for key in sorted(grouped):
        batches.extend(
            _batched(
                sorted(
                    grouped[key],
                    key=lambda item: (item.query_start, item.asset),
                ),
                batch_assets,
            )
        )
    return batches


def _fresh_frame_for_asset(
    asset: str,
    metrics: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    wide_rows: list[dict[str, Any]] = []
    vintage_rows: list[dict[str, Any]] = []
    for item in rows:
        if str(item.get("asset") or "") != asset:
            continue
        row_date = _date_text(item.get("time"))
        if row_date is None:
            continue
        wide: dict[str, Any] = {"date": row_date}
        for metric in metrics:
            if metric not in item:
                continue
            value = _float_or_none(item.get(metric))
            column = _metric_column(metric)
            wide[column] = value
            status = item.get(f"{metric}-status")
            status_time = item.get(f"{metric}-status-time")
            if status is not None:
                wide[f"{column}_status"] = str(status)
            if status_time is not None:
                wide[f"{column}_status_time"] = str(status_time)
            vintage_rows.append(
                {
                    "asset": asset,
                    "date": row_date,
                    "metric": metric,
                    "value": value,
                    "status": str(status) if status is not None else None,
                    "status_time_utc": (
                        str(status_time) if status_time is not None else None
                    ),
                }
            )
        wide_rows.append(wide)
    if not wide_rows:
        return pl.DataFrame(schema={"date": pl.String}), vintage_rows
    values = [
        column
        for column in sorted({key for row in wide_rows for key in row})
        if column != "date"
    ]
    fresh = pl.DataFrame(wide_rows, infer_schema_length=None)
    for column in values:
        if column not in fresh.columns:
            fresh = fresh.with_columns(pl.lit(None).alias(column))
    aggregations = [
        pl.col(column).drop_nulls().last().alias(column) for column in values
    ]
    return (
        fresh.group_by("date", maintain_order=True).agg(aggregations).sort("date"),
        vintage_rows,
    )


def _merge_wide(existing: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    if existing.is_empty():
        return fresh.sort("date")
    if fresh.is_empty():
        return existing.sort("date")
    combined = pl.concat(
        [
            existing.with_columns(pl.lit(0).alias("__priority")),
            fresh.with_columns(pl.lit(1).alias("__priority")),
        ],
        how="diagonal_relaxed",
    ).sort(["date", "__priority"])
    columns = [
        column for column in combined.columns if column not in {"date", "__priority"}
    ]
    return (
        combined.group_by("date", maintain_order=True)
        .agg([pl.col(column).drop_nulls().last().alias(column) for column in columns])
        .sort("date")
    )


def _append_vintages(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    observed_at: str,
    raw_sha256: str,
) -> int:
    existing = (
        pl.read_parquet(path)
        if path.is_file()
        else pl.DataFrame(
            schema={
                "asset": pl.String,
                "date": pl.String,
                "metric": pl.String,
                "value": pl.Float64,
                "status": pl.String,
                "status_time_utc": pl.String,
                "observed_at_utc": pl.String,
                "available_at_utc": pl.String,
                "raw_sha256": pl.String,
            }
        )
    )
    if not rows:
        return 0
    fresh = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        [
            pl.lit(observed_at).alias("observed_at_utc"),
            # Community history is revisioned. Unless the per-metric status
            # time proves a later publication, the current value first becomes
            # usable when this retrieval vintage was observed locally.
            pl.lit(observed_at).alias("available_at_utc"),
            pl.lit(raw_sha256).alias("raw_sha256"),
        ]
    )
    combined = pl.concat([existing, fresh], how="diagonal_relaxed").unique(
        subset=["asset", "date", "metric", "observed_at_utc"],
        keep="last",
        maintain_order=True,
    )
    added = max(0, combined.height - existing.height)
    _atomic_parquet(
        path,
        combined.sort(["observed_at_utc", "date", "metric"]),
    )
    return added


def _process_asset(
    spec: AssetSpec,
    batch_rows: list[dict[str, Any]],
    output_dir: Path,
    *,
    observed_at: str,
    raw_sha256: str,
) -> AssetResult:
    safe = _safe_component(spec.asset)
    output_path = output_dir / "assets" / f"{safe}_features.parquet"
    vintage_path = output_dir / "vintages" / f"{safe}_vintages.parquet"
    existing = pl.read_parquet(output_path) if output_path.is_file() else pl.DataFrame()
    fresh, vintage_rows = _fresh_frame_for_asset(spec.asset, spec.metrics, batch_rows)
    if fresh.is_empty() and existing.is_empty():
        return AssetResult(
            asset=spec.asset,
            status="unavailable_empty",
            metrics_registered=len(spec.metrics),
            metrics_with_data=0,
            rows=0,
            rows_added=0,
            vintage_rows_added=0,
            first_date=None,
            last_date=None,
            output_path=str(output_path),
            vintage_path=str(vintage_path),
            raw_sha256=raw_sha256,
            message="Catalog declared daily data but the requested range returned no rows.",
        )
    merged = _merge_wide(existing, fresh)
    for metric in spec.metrics:
        column = _metric_column(metric)
        if column not in merged.columns:
            merged = merged.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    if merged.select(pl.col("date").is_duplicated().any()).item():
        raise ValueError(f"duplicate Coin Metrics dates for {spec.asset}")
    numeric_columns = [
        _metric_column(metric)
        for metric in spec.metrics
        if _metric_column(metric) in merged.columns
    ]
    for column in numeric_columns:
        merged = merged.with_columns(pl.col(column).cast(pl.Float64, strict=False))
    invalid = sum(
        merged.filter(pl.col(column).is_not_null() & ~pl.col(column).is_finite()).height
        for column in numeric_columns
    )
    if invalid:
        raise ValueError(f"non-finite Coin Metrics values for {spec.asset}: {invalid}")
    rows_added = max(0, merged.height - existing.height)
    if existing.is_empty() or not existing.equals(merged):
        _atomic_parquet(output_path, merged)
    vintage_added = _append_vintages(
        vintage_path,
        vintage_rows,
        observed_at=observed_at,
        raw_sha256=raw_sha256,
    )
    metrics_with_data = sum(
        merged.filter(pl.col(column).is_not_null()).height > 0
        for column in numeric_columns
    )
    return AssetResult(
        asset=spec.asset,
        status="updated" if rows_added or vintage_added else "unchanged",
        metrics_registered=len(spec.metrics),
        metrics_with_data=metrics_with_data,
        rows=merged.height,
        rows_added=rows_added,
        vintage_rows_added=vintage_added,
        first_date=str(merged["date"].min()),
        last_date=str(merged["date"].max()),
        output_path=str(output_path),
        vintage_path=str(vintage_path),
        raw_sha256=raw_sha256,
    )


def _download_batch(
    client: CoinMetricsClient,
    batch: list[AssetSpec],
    output_dir: Path,
    *,
    requested_end: str,
) -> list[AssetResult]:
    assets = [spec.asset for spec in batch]
    metrics = batch[0].metrics
    start = min(spec.query_start for spec in batch)
    end = min(requested_end, max(spec.catalog_end for spec in batch))
    observed_at = datetime.now(timezone.utc).isoformat()
    rows, raw, _ = client.get_all(
        METRICS_ENDPOINT,
        {
            "assets": ",".join(assets),
            "metrics": ",".join(metrics),
            "frequency": FREQUENCY,
            "start_time": start,
            "end_time": end,
            "page_size": PAGE_SIZE,
        },
    )
    digest = _sha256_bytes(raw)
    run_key = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    raw_path = output_dir / "raw" / run_key[:8] / f"{run_key}-{digest[:16]}.json"
    _atomic_bytes(raw_path, raw)
    return [
        _process_asset(
            spec,
            rows,
            output_dir,
            observed_at=observed_at,
            raw_sha256=digest,
        )
        for spec in batch
    ]


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    start_date = date.fromisoformat(str(args.start_date)).isoformat()
    end_date = date.fromisoformat(resolve_end_date(args.end_date)).isoformat()
    if start_date > end_date:
        raise ValueError("start date is after end date")
    selected_assets = set(args.assets) if args.assets else None
    started = datetime.now(timezone.utc)
    progress_path = output_dir / "progress.json"
    client = CoinMetricsClient(
        max_retries=args.max_retries,
        retry_base=args.retry_base,
    )
    specs, catalog_raw = _catalog_specs(
        client,
        requested_start=start_date,
        requested_end=end_date,
        selected_assets=selected_assets,
        limit=args.limit,
    )
    if not specs:
        raise RuntimeError("Coin Metrics Community catalog selected no daily assets")
    catalog_digest = _sha256_bytes(catalog_raw)
    catalog_path = output_dir / "catalog" / "asset_metrics.json"
    _atomic_bytes(catalog_path, catalog_raw)
    batches = _build_batches(
        specs,
        output_dir,
        refresh=args.refresh,
        batch_assets=args.batch_assets,
    )
    results: list[AssetResult] = []
    results_lock = threading.Lock()
    progress = tqdm(
        total=len(specs), desc="download:coinmetrics-community", unit="asset"
    )

    def write_progress(state: str) -> None:
        observed = datetime.now(timezone.utc)
        completed = len(results)
        elapsed = max(0.001, (observed - started).total_seconds())
        rate = completed / elapsed if completed else 0.0
        remaining = len(specs) - completed
        eta_seconds = int(math.ceil(remaining / rate)) if rate > 0 else None
        counts: dict[str, int] = {}
        for item in results:
            counts[item.status] = counts.get(item.status, 0) + 1
        _atomic_json(
            progress_path,
            {
                "schema_version": SCHEMA_VERSION,
                "state": state,
                "label": "Coin Metrics Community 全量日資料",
                "current": completed,
                "total": len(specs),
                "unit": "asset",
                "ratio": completed / len(specs),
                "started_at_utc": started.isoformat(),
                "updated_at_utc": observed.isoformat(),
                "elapsed_seconds": elapsed,
                "items_per_second": rate,
                "remaining_seconds": eta_seconds,
                "estimated_complete_at_utc": (
                    (observed + timedelta(seconds=eta_seconds)).isoformat()
                    if eta_seconds is not None
                    else None
                ),
                "status_counts": counts,
                "basis": (
                    "completed assets divided by full elapsed time; catalog paging, "
                    "historical page depth and provider throttling can change the estimate"
                ),
            },
        )

    write_progress("running")

    def failed_batch(batch: list[AssetSpec], exc: Exception) -> list[AssetResult]:
        return [
            AssetResult(
                asset=spec.asset,
                status="failed",
                metrics_registered=len(spec.metrics),
                metrics_with_data=0,
                rows=0,
                rows_added=0,
                vintage_rows_added=0,
                first_date=None,
                last_date=None,
                output_path=str(
                    output_dir
                    / "assets"
                    / f"{_safe_component(spec.asset)}_features.parquet"
                ),
                vintage_path=str(
                    output_dir
                    / "vintages"
                    / f"{_safe_component(spec.asset)}_vintages.parquet"
                ),
                raw_sha256=None,
                message=f"{type(exc).__name__}: {exc}",
            )
            for spec in batch
        ]

    try:
        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
            futures = {
                executor.submit(
                    _download_batch,
                    client,
                    batch,
                    output_dir,
                    requested_end=end_date,
                ): batch
                for batch in batches
            }
            for future in as_completed(futures):
                batch = futures[future]
                try:
                    batch_results = future.result()
                except Exception as exc:
                    batch_results = failed_batch(batch, exc)
                with results_lock:
                    results.extend(batch_results)
                    progress.update(len(batch_results))
                    progress.set_postfix(
                        updated=sum(
                            item.status in {"updated", "unchanged"} for item in results
                        ),
                        failed=sum(item.status == "failed" for item in results),
                        refresh=False,
                    )
                    write_progress("running")
    finally:
        progress.close()

    results.sort(key=lambda item: item.asset)
    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"
    receipt_path = output_dir / "download_receipt.json"
    report_csv = pl.DataFrame(
        [asdict(result) for result in results], infer_schema_length=None
    ).write_csv()
    _atomic_bytes(
        report_path,
        report_csv.encode("utf-8"),
    )
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    ended = datetime.now(timezone.utc)
    summary = {
        "asset_class": "coinmetrics_community_daily",
        "frequency": FREQUENCY,
        "asset_count": len(specs),
        "metric_registration_count": sum(len(spec.metrics) for spec in specs),
        "unique_metric_count": len(
            {metric for spec in specs for metric in spec.metrics}
        ),
        "status_counts": status_counts,
        "row_count": sum(result.rows for result in results),
        "rows_added": sum(result.rows_added for result in results),
        "vintage_rows_added": sum(result.vintage_rows_added for result in results),
        "start_date": start_date,
        "end_date": end_date,
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "elapsed_seconds": (ended - started).total_seconds(),
        "catalog_sha256": catalog_digest,
        "community_rate_limit_requests_per_second": provider_rate_limit(
            "coinmetrics_community"
        ).requests_per_second,
        "point_in_time_contract": (
            "Latest-view asset parquets are research storage. The per-asset "
            "vintage parquets are canonical for causal use: a value is unavailable "
            "before its observed_at_utc, and revisions never overwrite old vintages."
        ),
        "license_boundary": (
            "Coin Metrics Community is free without an API key but carries its own "
            "Creative Commons/non-commercial terms; do not equate free access with "
            "unrestricted redistribution."
        ),
    }
    _atomic_json(summary_path, summary)
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": ended.isoformat(),
        "source": {
            "provider": "Coin Metrics Community",
            "base_url": BASE_URL,
            "catalog_endpoint": CATALOG_ENDPOINT,
            "metrics_endpoint": METRICS_ENDPOINT,
            "documentation": "https://docs.coinmetrics.io/api/v4/",
        },
        "artifacts": {
            path.name: {
                "bytes": path.stat().st_size,
                "sha256": _sha256_path(path),
            }
            for path in (catalog_path, report_path, summary_path)
        },
        "status_counts": status_counts,
    }
    _atomic_json(receipt_path, receipt)
    write_progress("failed" if status_counts.get("failed") else "complete")
    print(f"[coinmetrics] catalog -> {catalog_path}")
    print(f"[coinmetrics] report -> {report_path}")
    print(f"[coinmetrics] summary -> {summary_path}")
    print(f"[coinmetrics] receipt -> {receipt_path}")
    print(f"[coinmetrics] done: {json.dumps(summary, ensure_ascii=False)}")
    if status_counts.get("failed"):
        raise RuntimeError(
            f"Coin Metrics Community incomplete: {status_counts['failed']} assets failed"
        )


if __name__ == "__main__":
    main()
