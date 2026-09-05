"""Download small, auditable historical public crypto sidecars.

These datasets deliberately remain outside the trained feature ABI.  Both
sources expose old observations today, but neither supplies a complete revision
or historical-release calendar.  The normalized tables therefore preserve two
different clocks:

* ``assumed_available_at_utc`` is a documented research approximation;
* ``strict_available_at_utc`` is the first time this workspace observed it.

Only the strict clock is safe without a separately audited release-calendar
contract.  This prevents a historical value downloaded today from silently
becoming information that the model supposedly knew years ago.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import gzip
import json
from pathlib import Path
import sys
from typing import Any, Iterable
from urllib.parse import quote, urlencode

import polars as pl


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from artifact_io import (  # noqa: E402
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_parquet,
    sha256_bytes,
)
from common import SharedRateLimiter  # noqa: E402
from http_transport import HttpRequestPolicy, ResilientHttpTransport  # noqa: E402


SCHEMA_VERSION = 1
CFTC_ENDPOINT = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
WIKIMEDIA_ENDPOINT = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia/all-access/user/{article}/daily/{start}/{end}"
)
DEFAULT_ARTICLES = (
    "Bitcoin",
    "Ethereum",
    "Cryptocurrency",
    "Blockchain",
    "Stablecoin",
    "Decentralized_finance",
    "Tether_(cryptocurrency)",
    "USD_Coin",
)
CFTC_TEXT_COLUMNS = {
    "id",
    "market_and_exchange_names",
    "report_date_as_yyyy_mm_dd",
    "yyyy_report_week_ww",
    "contract_market_name",
    "cftc_contract_market_code",
    "cftc_market_code",
    "cftc_region_code",
    "cftc_commodity_code",
    "commodity_name",
    "contract_units",
    "cftc_subgroup_code",
    "commodity",
    "commodity_subgroup_name",
    "commodity_group_name",
    "futonly_or_combined",
}
CFTC_NUMERIC_PREFIXES = (
    "open_interest",
    "dealer_",
    "asset_mgr_",
    "lev_money_",
    "other_rept_",
    "tot_rept_",
    "nonrept_",
    "change_in_",
    "pct_of_",
    "traders_",
    "conc_",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_date(value: str, *, today: date) -> date:
    if value.casefold() == "today":
        return today
    return date.fromisoformat(value)


def _gzip(payload: bytes) -> bytes:
    return gzip.compress(payload, compresslevel=9, mtime=0)


def _write_raw(path: Path, payload: bytes) -> dict[str, Any]:
    compressed = _gzip(payload)
    atomic_write_bytes(path, compressed)
    return {
        "path": str(path),
        "uncompressed_bytes": len(payload),
        "compressed_bytes": len(compressed),
        "sha256_uncompressed": sha256_bytes(payload),
        "sha256_compressed": sha256_bytes(compressed),
    }


def _cftc_url(start: date, end: date) -> str:
    where = (
        "commodity_subgroup_name='DIGITAL ASSET' AND "
        f"report_date_as_yyyy_mm_dd >= '{start.isoformat()}T00:00:00.000' AND "
        f"report_date_as_yyyy_mm_dd <= '{end.isoformat()}T23:59:59.999'"
    )
    return CFTC_ENDPOINT + "?" + urlencode(
        {"$limit": "50000", "$order": "report_date_as_yyyy_mm_dd,id", "$where": where}
    )


def _numeric_columns(rows: list[dict[str, Any]]) -> list[str]:
    names = {key for row in rows for key in row}
    return sorted(
        name
        for name in names
        if name not in CFTC_TEXT_COLUMNS
        and any(name.startswith(prefix) for prefix in CFTC_NUMERIC_PREFIXES)
    )


def _net_expr(long_col: str, short_col: str, output: str) -> pl.Expr:
    return (pl.col(long_col).fill_null(0.0) - pl.col(short_col).fill_null(0.0)).alias(output)


def normalize_cftc(rows: list[dict[str, Any]], observed_at: datetime) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    numeric = _numeric_columns(rows)
    frame = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.col("report_date_as_yyyy_mm_dd")
        .str.slice(0, 10)
        .str.to_date("%Y-%m-%d", strict=True)
        .cast(pl.Datetime(time_zone="UTC"))
        .alias("report_date_utc"),
        *[pl.col(name).cast(pl.Float64, strict=False) for name in numeric],
    )
    # Weekly reports describe Tuesday positions and are normally published on
    # Friday.  Saturday 00:00 UTC is intentionally conservative but not an
    # authoritative holiday/shutdown calendar, so it remains an assumption.
    frame = frame.with_columns(
        (pl.col("report_date_utc") + pl.duration(days=4)).alias(
            "assumed_available_at_utc"
        ),
        pl.lit(observed_at).alias("strict_available_at_utc"),
        pl.lit("historical_archive_first_observed_now").alias("point_in_time_state"),
        pl.lit("quarantined_until_release_calendar_audit").alias("causal_use_status"),
    )
    pairs = (
        ("dealer_positions_long_all", "dealer_positions_short_all", "dealer_net"),
        ("asset_mgr_positions_long", "asset_mgr_positions_short", "asset_manager_net"),
        ("lev_money_positions_long", "lev_money_positions_short", "leveraged_money_net"),
        ("other_rept_positions_long", "other_rept_positions_short", "other_reportable_net"),
        ("nonrept_positions_long_all", "nonrept_positions_short_all", "nonreportable_net"),
    )
    existing = set(frame.columns)
    frame = frame.with_columns(
        *[_net_expr(long, short, output) for long, short, output in pairs if {long, short} <= existing]
    )
    net_columns = [output for long, short, output in pairs if {long, short} <= existing]
    if "open_interest_all" in existing:
        frame = frame.with_columns(
            *[
                pl.when(pl.col("open_interest_all") > 0)
                .then(pl.col(name) / pl.col("open_interest_all"))
                .otherwise(None)
                .alias(f"{name}_fraction")
                for name in net_columns
            ]
        )
    return frame.sort(["report_date_utc", "cftc_contract_market_code", "id"])


def normalize_wikimedia(
    items: list[dict[str, Any]], article: str, observed_at: datetime
) -> pl.DataFrame:
    if not items:
        return pl.DataFrame()
    return (
        pl.DataFrame(items, infer_schema_length=None)
        .with_columns(
            pl.lit(article).alias("requested_article"),
            pl.col("timestamp")
            .cast(pl.String)
            .str.slice(0, 8)
            .str.to_date("%Y%m%d", strict=True)
            .cast(pl.Datetime(time_zone="UTC"))
            .alias("event_date_utc"),
            pl.col("views").cast(pl.Int64, strict=False),
        )
        .with_columns(
            (pl.col("event_date_utc") + pl.duration(days=2)).alias(
                "assumed_available_at_utc"
            ),
            pl.lit(observed_at).alias("strict_available_at_utc"),
            pl.lit("historical_archive_first_observed_now").alias("point_in_time_state"),
            pl.lit("research_only_until_revision_and_redirect_audit").alias(
                "causal_use_status"
            ),
        )
        .select(
            "requested_article",
            "project",
            "article",
            "granularity",
            "access",
            "agent",
            "event_date_utc",
            "views",
            "assumed_available_at_utc",
            "strict_available_at_utc",
            "point_in_time_state",
            "causal_use_status",
        )
        .sort(["event_date_utc", "requested_article"])
    )


def _profile(frame: pl.DataFrame, keys: Iterable[str], time_col: str) -> dict[str, Any]:
    key_list = list(keys)
    duplicates = (
        frame.group_by(key_list).len().filter(pl.col("len") > 1).select(pl.col("len").sum()).item()
        if frame.height
        else 0
    )
    return {
        "rows": frame.height,
        "columns": len(frame.columns),
        "key": key_list,
        "duplicate_rows_beyond_first": int(duplicates or 0),
        "first_event_utc": frame.select(pl.col(time_col).min()).item().isoformat(),
        "last_event_utc": frame.select(pl.col(time_col).max()).item().isoformat(),
        "null_counts": {
            name: int(value)
            for name, value in zip(frame.columns, frame.null_count().row(0), strict=True)
            if value
        },
    }


def download_cftc(
    output_dir: Path,
    start: date,
    end: date,
    observed_at: datetime,
    transport: ResilientHttpTransport,
) -> dict[str, Any]:
    response = transport.request_bytes(
        _cftc_url(start, end),
        headers={"Accept": "application/json", "User-Agent": "stockAgent-public-data/1.0"},
    )
    rows = json.loads(response.body)
    if not isinstance(rows, list):
        raise RuntimeError("CFTC response is not a JSON row array")
    raw = _write_raw(output_dir / "raw/cftc/tff_digital_assets.json.gz", response.body)
    frame = normalize_cftc(rows, observed_at)
    if frame.is_empty():
        raise RuntimeError("CFTC returned no digital-asset rows")
    destination = output_dir / "normalized/cftc_tff_digital_assets.parquet"
    atomic_write_parquet(destination, frame, row_group_size=min(10_000, frame.height))
    profile = _profile(frame, ("id",), "report_date_utc")
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "source": "CFTC Traders in Financial Futures",
        "status": "complete_research_sidecar",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "observed_at_utc": _iso(observed_at),
        "raw": raw,
        "normalized_path": str(destination),
        "quality": profile,
        "availability_contract": {
            "strict": "local first observation timestamp",
            "assumed": "report date plus four days; not holiday/shutdown authoritative",
            "training_status": "quarantined until release-calendar audit",
        },
    }
    atomic_write_json(output_dir / "receipts/cftc.json", receipt)
    return receipt


def _wiki_url(article: str, start: date, end: date) -> str:
    return WIKIMEDIA_ENDPOINT.format(
        article=quote(article, safe=""),
        start=start.strftime("%Y%m%d00"),
        end=end.strftime("%Y%m%d00"),
    )


def download_wikimedia(
    output_dir: Path,
    start: date,
    end: date,
    observed_at: datetime,
    transport: ResilientHttpTransport,
    articles: Iterable[str] = DEFAULT_ARTICLES,
) -> dict[str, Any]:
    frames: list[pl.DataFrame] = []
    raw_receipts: list[dict[str, Any]] = []
    for article in articles:
        response = transport.request_bytes(
            _wiki_url(article, start, end),
            headers={"Accept": "application/json", "User-Agent": "stockAgent-public-data/1.0"},
        )
        payload = json.loads(response.body)
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list) or not items:
            raise RuntimeError(f"Wikimedia returned no rows for {article}")
        raw_receipts.append(
            _write_raw(output_dir / f"raw/wikimedia/{article}.json.gz", response.body)
        )
        frames.append(normalize_wikimedia(items, article, observed_at))
    frame = pl.concat(frames, how="vertical").sort(["event_date_utc", "requested_article"])
    destination = output_dir / "normalized/wikimedia_crypto_pageviews_daily.parquet"
    atomic_write_parquet(destination, frame, row_group_size=min(10_000, frame.height))
    profile = _profile(frame, ("requested_article", "event_date_utc"), "event_date_utc")
    coverage = (
        frame.group_by("requested_article")
        .agg(
            pl.len().alias("rows"),
            pl.col("event_date_utc").min().alias("first_event_utc"),
            pl.col("event_date_utc").max().alias("last_event_utc"),
            pl.col("views").null_count().alias("null_views"),
        )
        .sort("requested_article")
    )
    coverage_path = output_dir / "quality/wikimedia_article_coverage.csv"
    atomic_write_bytes(coverage_path, coverage.write_csv().encode("utf-8"))
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "source": "Wikimedia REST API pageviews",
        "status": "complete_research_sidecar",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "observed_at_utc": _iso(observed_at),
        "articles": list(articles),
        "raw": raw_receipts,
        "normalized_path": str(destination),
        "coverage_path": str(coverage_path),
        "quality": profile,
        "availability_contract": {
            "strict": "local first observation timestamp",
            "assumed": "event day plus two days; not a revision-vintage guarantee",
            "training_status": "research-only until revision and redirect audit",
        },
    }
    atomic_write_json(output_dir / "receipts/wikimedia.json", receipt)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_crypto_historical_public"))
    parser.add_argument("--start-date", default="2015-07-01")
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--sources", nargs="+", choices=("cftc", "wikimedia"), default=["cftc", "wikimedia"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    today = _utc_now().date()
    start = _parse_date(args.start_date, today=today)
    requested_end = _parse_date(args.end_date, today=today)
    end = min(requested_end, today - timedelta(days=1))
    if end < start:
        raise ValueError(f"invalid date range: {start}..{end}")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (output_dir / ".download.lock").open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(f"another updater owns {output_dir / '.download.lock'}")

    observed_at = _utc_now()
    transport = ResilientHttpTransport(
        HttpRequestPolicy(provider="crypto_historical_public", timeout_seconds=90, max_retries=5),
        limiter=SharedRateLimiter(0.25, name="crypto_historical_public"),
    )
    results: dict[str, Any] = {}
    if "cftc" in args.sources:
        results["cftc"] = download_cftc(output_dir, start, end, observed_at, transport)
    if "wikimedia" in args.sources:
        results["wikimedia"] = download_wikimedia(output_dir, start, end, observed_at, transport)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "state": "complete",
        "requested_start": start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "effective_end": end.isoformat(),
        "observed_at_utc": _iso(observed_at),
        "sources": results,
        "feature_abi_status": "not_integrated_research_sidecar",
    }
    atomic_write_json(output_dir / "download_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
