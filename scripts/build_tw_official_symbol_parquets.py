from __future__ import annotations

import argparse
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

import numpy as np
import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_public_features import (
    _date_column_expr,
    _file_content_receipt,
    _num_expr,
    _symbol_expr,
)
from stockagent.data.tw_security import (
    TW_ETF_SYMBOL_PATTERN,
    TW_STOCK_SYMBOL_PATTERN,
    classify_tw_stock_or_etf,
)


OFFICIAL_SOURCE_NAME = "twse_tpex_official"
MIXED_FALLBACK_SOURCE_NAME = "twse_tpex_official_with_yahoo_fallback"
YAHOO_FALLBACK_SOURCE_NAME = "yahoo_fallback"
OFFICIAL_SYMBOL_BUILD_SCHEMA_VERSION = 6
OFFICIAL_SYMBOL_ADJUSTED_PRICE_METHOD = (
    "first=10 per official lifecycle episode; next=previous_index*factor; "
    "factor=official/legacy explicit ratio else corporate_action_reference "
    "ratio else close/(close-signed_change) else close/previous official "
    "session next_reference else Yahoo source-adjusted ratio"
)
PREVIOUS_SESSION_NEXT_REFERENCE_SOURCE_NAME = (
    "tpex_official_previous_session_next_reference"
)
RETURN_QUARANTINE_REASONS = frozenset(
    {
        "official_lifecycle_episode_boundary",
        "official_listing_boundary",
        "unverified_extreme_adjusted_return",
    }
)
OFFICIAL_CORE_SOURCE_FILENAMES = (
    "twse_daily_ohlcv.parquet",
    "tpex_daily_ohlcv.parquet",
    "tw_corporate_action_reference.parquet",
)
LIFECYCLE_EVIDENCE_FILENAMES = (
    "twse_delisted_company.parquet",
    "tpex_delisted_company.parquet",
    "twse_listed_company_basic.parquet",
    "tpex_basic_company.parquet",
    "twse_api_company_newlisting.parquet",
)
LEGACY_COLUMN_ALIASES = {
    "code": "symbol",
    "security_code": "symbol",
    "venue": "market",
    "exchange": "market",
    "high": "max",
    "low": "min",
    "volume": "Trading_Volume",
    "change": "signed_change",
    "price_change": "signed_change",
    "adjusted_close": "source_adjclose",
    "adj_close": "source_adjclose",
    "adjclose": "source_adjclose",
    "reference_price": "source_reference",
    "opening_reference_price": "source_reference",
    "adjustment_factor": "source_factor",
}
PARQUET_METADATA = {
    "source": b"stockagent.source",
    "asset_class": b"stockagent.asset_class",
    "checked_through": b"stockagent.official_checked_through",
    "first_date": b"stockagent.first_date",
    "last_date": b"stockagent.last_date",
    "write_ts_utc": b"stockagent.write_ts_utc",
}
YAHOO_INPUT_METADATA = {
    "source": b"stockagent.source",
    "asset_class": b"stockagent.asset_class",
    "requested_start": b"stockagent.yahoo_requested_start",
    "checked_through": b"stockagent.yahoo_checked_through",
}


def _official_symbol_source_paths(input_dir: Path) -> list[Path]:
    """Return source files in the exact order recorded by the build summary."""

    return [
        *(input_dir / name for name in OFFICIAL_CORE_SOURCE_FILENAMES),
        *(
            path
            for name in LIFECYCLE_EVIDENCE_FILENAMES
            if (path := input_dir / name).is_file()
        ),
    ]


@dataclass(slots=True)
class BuildResult:
    symbol: str
    security_type: str
    market: str
    name: str
    status: str
    rows: int
    adjusted_rows: int
    missing_adjustment_rows: int
    first_date: str | None
    last_date: str | None
    output_path: str
    source: str = OFFICIAL_SOURCE_NAME
    fallback_rows: int = 0
    fallback_adjustment_rows: int = 0
    previous_session_next_reference_adjustment_rows: int = 0
    normalized_zero_ohlc_rows: int = 0
    return_quarantined_rows: int = 0
    lifecycle_episode_quarantined_rows: int = 0
    listing_boundary_quarantined_rows: int = 0
    unverified_extreme_quarantined_rows: int = 0
    message: str | None = None


@dataclass(slots=True)
class OfficialMergeStats:
    official_unusable_ohlcv_rows: int
    fallback_replaced_unusable_official_rows: int
    unfilled_unusable_official_rows: int
    unfilled_unusable_official_examples: list[str]
    session_calendar_rows: int
    session_calendar_receipt: dict[str, str | int]
    session_calendar_summary_receipt: dict[str, str | int]
    dropped_off_calendar_fallback_rows: int
    dropped_off_calendar_fallback_examples: list[str]
    fallback_rows_before_lifecycle_filter: int
    fallback_rows_after_lifecycle_filter: int
    dropped_post_terminal_fallback_rows: int
    dropped_post_terminal_fallback_examples: list[str]
    lifecycle_terminal_events: int
    lifecycle_nonterminal_transfer_events: int
    lifecycle_nonterminal_transfer_examples: list[str]
    lifecycle_source_receipts: list[dict[str, Any]]
    previous_session_next_reference_candidate_rows: int = 0
    previous_session_next_reference_candidate_examples: list[str] | None = None


@dataclass(slots=True)
class OfficialLifecyclePolicy:
    terminal_intervals: pl.DataFrame
    listing_boundaries: pl.DataFrame
    nonterminal_transfer_examples: list[str]
    source_receipts: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build canonical per-symbol TW parquet files with official TWSE/TPEx rows at "
            "highest priority and an explicitly identified lower-priority fallback. adjclose "
            "is a reference-return index normalized to 10 at each symbol's first row."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--end-date",
        default=None,
        help=(
            "Inclusive completed-session cutoff (YYYY-MM-DD). Source rows after this "
            "date are ignored so a partial future download cannot contaminate the build."
        ),
    )
    parser.add_argument(
        "--legacy-official-ohlcv",
        type=Path,
        action="append",
        default=[],
        help=(
            "Provenance-backed TWSE/TPEx archive to merge before index construction; "
            "repeatable and may provide adjclose, signed_change, or reference_price."
        ),
    )
    parser.add_argument("--legacy-source-name", default=None)
    parser.add_argument(
        "--fallback-ohlcv",
        type=Path,
        action="append",
        default=[],
        help="Lower-priority OHLCV fallback archive; repeatable. Official date-symbol rows always win.",
    )
    parser.add_argument("--fallback-source-name", default=None)
    parser.add_argument("--summary-path", type=Path, default=None)
    parser.add_argument("--report-path", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-incomplete-source",
        action="store_true",
        help="Diagnostics only: permit non-full public download receipts.",
    )
    parser.add_argument(
        "--allow-daily-publication-lag",
        action="store_true",
        help=(
            "Accept a certified daily-close receipt whose only missing current-day "
            "datasets are the late TWSE/TPEx margin-balance reports."
        ),
    )
    return parser.parse_args()


def _validate_download_receipts(
    input_dir: Path,
    *,
    allow_incomplete: bool,
    allow_daily_publication_lag: bool = False,
) -> None:
    public_summary_path = input_dir / "download_summary.json"
    corporate_summary_path = input_dir / "tw_corporate_action_reference.summary.json"
    problems: list[str] = []
    try:
        public_summary = json.loads(public_summary_path.read_text(encoding="utf-8"))
        legacy_full_receipt = (
            "coverage_complete" not in public_summary
            and public_summary.get("mode") == "full"
        )
        certified_daily_close = (
            allow_daily_publication_lag
            and public_summary.get("mode") == "daily"
            and public_summary.get("daily_close_ready") is True
            and int(public_summary.get("blocking_failed_count", -1)) == 0
            and set(public_summary.get("publication_lag_datasets") or ())
            <= {"twse_margin_balance", "tpex_margin_balance"}
        )
        if (
            public_summary.get("coverage_complete") is not True
            and not legacy_full_receipt
            and not certified_daily_close
        ):
            problems.append(
                "public historical coverage is incomplete "
                f"(mode={public_summary.get('mode')!r}, "
                f"coverage_complete={public_summary.get('coverage_complete')!r})"
            )
        if (
            int(public_summary.get("failed_count", -1)) != 0
            and not certified_daily_close
        ):
            problems.append(f"public failed_count={public_summary.get('failed_count')!r}")
    except Exception as exc:
        problems.append(f"invalid public receipt: {type(exc).__name__}: {exc}")
    try:
        corporate_summary = json.loads(corporate_summary_path.read_text(encoding="utf-8"))
        if int(corporate_summary.get("failure_count", -1)) != 0:
            problems.append(
                f"corporate-action failure_count={corporate_summary.get('failure_count')!r}"
            )
        if (
            int(corporate_summary.get("schema_version", 1)) >= 2
            and corporate_summary.get("coverage_complete") is not True
        ):
            problems.append("corporate-action historical coverage is incomplete")
        output_path = input_dir / "tw_corporate_action_reference.parquet"
        if corporate_summary.get("output_receipt") != _receipt(output_path):
            problems.append("corporate-action output receipt mismatch")
    except Exception as exc:
        problems.append(f"invalid corporate-action receipt: {type(exc).__name__}: {exc}")
    if problems and not allow_incomplete:
        raise RuntimeError(
            "official symbol build requires complete source receipts: " + "; ".join(problems)
        )


def _receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "size": int(path.stat().st_size), "sha256": digest.hexdigest()}


def _receipt_matches(path: Path, receipt: Any) -> bool:
    if not isinstance(receipt, dict):
        return False
    try:
        actual = _receipt(path)
        return (
            int(receipt["size"]) == actual["size"]
            and str(receipt["sha256"]) == actual["sha256"]
        )
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _load_verified_taiex_session_calendar(
    input_dir: Path,
) -> tuple[pl.DataFrame, dict[str, str | int], dict[str, str | int]]:
    """Load the receipt-verified official TAIEX actual-session calendar."""

    path = input_dir / "twse_taiex_ohlc.parquet"
    summary_path = path.with_suffix(".summary.json")
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"missing or invalid TAIEX session-calendar receipt {summary_path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(summary, dict):
        raise RuntimeError(f"TAIEX session-calendar receipt is not an object: {summary_path}")

    expected_columns = {
        "date",
        "opening_index",
        "highest_index",
        "lowest_index",
        "closing_index",
        "_dataset",
        "_source",
        "_source_product",
        "_request_month",
        "_downloaded_at_utc",
        "_url",
    }
    try:
        schema = set(pq.read_schema(path).names)
        frame = pl.read_parquet(
            path,
            columns=[
                "date",
                "opening_index",
                "highest_index",
                "lowest_index",
                "closing_index",
            ],
        ).select(
            _date_column_expr("date").alias("date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).fill_nan(None).alias(column)
                for column in (
                    "opening_index",
                    "highest_index",
                    "lowest_index",
                    "closing_index",
                )
            ],
        )
    except Exception as exc:
        raise RuntimeError(
            f"TAIEX session calendar is unreadable {path}: {type(exc).__name__}: {exc}"
        ) from exc

    valid_ohlc = (
        pl.col("date").is_not_null()
        & pl.all_horizontal(
            *[
                pl.col(column).is_not_null()
                & pl.col(column).is_finite()
                & (pl.col(column) > 0.0)
                for column in (
                    "opening_index",
                    "highest_index",
                    "lowest_index",
                    "closing_index",
                )
            ]
        )
        & (
            pl.col("highest_index")
            >= pl.max_horizontal("opening_index", "lowest_index", "closing_index")
        )
        & (
            pl.col("lowest_index")
            <= pl.min_horizontal("opening_index", "highest_index", "closing_index")
        )
    )
    invalid_rows = int(frame.select((~valid_ohlc).fill_null(True).sum()).item() or 0)
    dates = frame.select("date").drop_nulls().sort("date")
    first_date = str(dates["date"].min() or "")[:10] if dates.height else ""
    checks = {
        "identity": (
            summary.get("dataset") == "twse_taiex_ohlc"
            and summary.get("source") == "TWSE"
        ),
        "official_start": (
            summary.get("official_start_date") == "1999-01-05"
            and summary.get("effective_start_date") == "1999-01-05"
            and first_date == "1999-01-05"
        ),
        "coverage_complete": summary.get("coverage_complete") is True,
        "baseline_established": summary.get("baseline_established") is True,
        "replacement_promoted": summary.get("replacement_promoted") is True,
        "no_failures": (
            int(summary.get("failed_count", -1)) == 0
            and int(summary.get("unresolved_month_count", -1)) == 0
        ),
        "schema": expected_columns <= schema,
        "rows": (
            dates.height > 0
            and dates.height == dates["date"].n_unique()
            and dates.height == int(summary.get("output_rows", -1))
        ),
        "ohlc": invalid_rows == 0,
        "output_receipt": _receipt_matches(path, summary.get("output_receipt")),
    }
    public_summary_path = input_dir / "download_summary.json"
    if public_summary_path.is_file():
        try:
            public_summary = json.loads(public_summary_path.read_text(encoding="utf-8"))
            public_end = date.fromisoformat(str(public_summary.get("end_date", ""))[:10])
            calendar_end = date.fromisoformat(
                str(summary.get("effective_end_date", ""))[:10]
            )
            checks["date_range_matches_public"] = calendar_end >= public_end
        except (AttributeError, json.JSONDecodeError, OSError, TypeError, ValueError):
            checks["date_range_matches_public"] = False
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(
            f"TAIEX session-calendar receipt failed checks {failed}: {summary_path}"
        )
    return (
        dates.unique("date", keep="first", maintain_order=True),
        _file_content_receipt(path),
        _file_content_receipt(summary_path),
    )


def _parse_official_lifecycle_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    digits = "".join(character for character in text if character.isdigit())
    try:
        if len(digits) == 8 and int(digits[:4]) >= 1911:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        if len(digits) == 7:
            return date(
                int(digits[:3]) + 1911,
                int(digits[3:5]),
                int(digits[5:7]),
            )
    except ValueError:
        return None
    return None


def _empty_lifecycle_intervals() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "symbol": pl.String,
            "terminal_date": pl.Date,
            "terminal_market": pl.String,
            "terminal_name": pl.String,
            "terminal_source": pl.String,
            "reopen_date": pl.Date,
            "reopen_market": pl.String,
            "reopen_source": pl.String,
            "episode_id": pl.Int32,
        }
    )


def _empty_listing_boundaries() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "symbol": pl.String,
            "date": pl.Date,
            "listing_evidence": pl.String,
        }
    )


def _load_official_lifecycle_policy(
    input_dir: Path,
    current_official: pl.DataFrame,
    session_calendar: pl.DataFrame,
    *,
    required: bool,
) -> OfficialLifecyclePolicy:
    paths = [input_dir / name for name in LIFECYCLE_EVIDENCE_FILENAMES]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        if required:
            raise FileNotFoundError(
                "Yahoo fallback requires complete official lifecycle evidence; "
                f"missing={missing}"
            )
        return OfficialLifecyclePolicy(
            terminal_intervals=_empty_lifecycle_intervals(),
            listing_boundaries=_empty_listing_boundaries(),
            nonterminal_transfer_examples=[],
            source_receipts=[],
        )

    receipts = [_file_content_receipt(path) for path in paths]
    sessions = session_calendar.get_column("date").sort().to_list()

    def following_official_session(value: date) -> date | None:
        index = bisect_right(sessions, value)
        return sessions[index] if index < len(sessions) else None

    delistings: list[dict[str, Any]] = []
    for filename, default_market in (
        ("twse_delisted_company.parquet", "twse"),
        ("tpex_delisted_company.parquet", "tpex"),
    ):
        frame = pl.read_parquet(input_dir / filename)
        required_columns = {"date", "symbol"}
        missing_columns = sorted(required_columns - set(frame.columns))
        if missing_columns:
            raise ValueError(
                f"{filename} is missing lifecycle columns: {missing_columns}"
            )
        for row in frame.iter_rows(named=True):
            event_date = _parse_official_lifecycle_date(row.get("date"))
            symbol = str(row.get("symbol") or "").strip().upper()
            if event_date is None or classify_tw_stock_or_etf(symbol) is None:
                continue
            delistings.append(
                {
                    "symbol": symbol,
                    "date": event_date,
                    "market": str(row.get("market") or default_market).strip().lower(),
                    "name": str(row.get("company_name") or symbol).strip(),
                    "source": filename.removesuffix(".parquet"),
                }
            )

    listing_events: list[dict[str, Any]] = []

    def add_basic_listings(
        filename: str,
        *,
        symbol_column: str,
        listing_column: str,
        name_columns: tuple[str, ...],
        market: str,
    ) -> None:
        frame = pl.read_parquet(input_dir / filename)
        required_columns = {symbol_column, listing_column}
        missing_columns = sorted(required_columns - set(frame.columns))
        if missing_columns:
            raise ValueError(
                f"{filename} is missing lifecycle columns: {missing_columns}"
            )
        for row in frame.iter_rows(named=True):
            symbol = str(row.get(symbol_column) or "").strip().upper()
            listing_date = _parse_official_lifecycle_date(row.get(listing_column))
            if listing_date is None or classify_tw_stock_or_etf(symbol) is None:
                continue
            name = next(
                (
                    str(row.get(column) or "").strip()
                    for column in name_columns
                    if str(row.get(column) or "").strip()
                ),
                symbol,
            )
            listing_events.append(
                {
                    "symbol": symbol,
                    "date": listing_date,
                    "market": market,
                    "name": name,
                    "source": filename.removesuffix(".parquet"),
                }
            )

    add_basic_listings(
        "twse_listed_company_basic.parquet",
        symbol_column="公司代號",
        listing_column="上市日期",
        name_columns=("公司簡稱", "公司名稱"),
        market="twse",
    )
    add_basic_listings(
        "tpex_basic_company.parquet",
        symbol_column="SecuritiesCompanyCode",
        listing_column="DateOfListing",
        name_columns=("CompanyAbbreviation", "CompanyName"),
        market="tpex",
    )

    newlisting = pl.read_parquet(input_dir / "twse_api_company_newlisting.parquet")
    if {"Code", "Note"} - set(newlisting.columns):
        raise ValueError(
            "twse_api_company_newlisting.parquet is missing Code/Note lifecycle columns"
        )
    transfer_symbols = {
        str(row.get("Code") or "").strip().upper()
        for row in newlisting.iter_rows(named=True)
        if "櫃轉市" in str(row.get("Note") or "")
    }
    proposed_newlisting_events: list[dict[str, Any]] = []
    for row in newlisting.iter_rows(named=True):
        symbol = str(row.get("Code") or "").strip().upper()
        if classify_tw_stock_or_etf(symbol) is None:
            continue
        listing_date = None
        for column in ("ListingDate", "ApprovedListingDate"):
            if column in newlisting.columns:
                listing_date = _parse_official_lifecycle_date(row.get(column))
                if listing_date is not None:
                    break
        if listing_date is None:
            continue
        proposed_newlisting_events.append(
            {
                "symbol": symbol,
                "date": listing_date,
                "market": "twse",
                "name": str(row.get("Company") or symbol).strip(),
                "source": "twse_api_company_newlisting",
            }
        )

    if proposed_newlisting_events:
        proposed = pl.DataFrame(proposed_newlisting_events).with_columns(
            pl.col("date").cast(pl.Date)
        )
        quote_dates = (
            current_official.select(
                "symbol",
                "market",
                pl.col("date").alias("quote_date"),
            )
            .unique()
            .sort(["symbol", "market", "quote_date"])
        )
        confirmed = (
            proposed.sort(["symbol", "market", "date"])
            .join_asof(
                quote_dates,
                left_on="date",
                right_on="quote_date",
                by=["symbol", "market"],
                strategy="forward",
                check_sortedness=False,
            )
            .filter(
                pl.col("quote_date").is_not_null()
                & (
                    (pl.col("quote_date") == pl.col("date"))
                    | pl.struct("date", "quote_date").map_elements(
                        lambda row: following_official_session(row["date"])
                        == row["quote_date"],
                        return_dtype=pl.Boolean,
                    )
                )
            )
            .drop("quote_date")
        )
        listing_events.extend(confirmed.to_dicts())

    if delistings and not current_official.is_empty():
        delisting_frame = pl.DataFrame(delistings).select(
            "symbol",
            pl.col("date").cast(pl.Date),
            pl.col("market").alias("delisting_market"),
        )
        quote_dates = (
            current_official.select(
                "symbol",
                pl.col("date").alias("quote_date"),
                pl.col("market").alias("quote_market"),
                "name",
            )
            .unique(["symbol", "quote_date", "quote_market"], keep="last")
        )
        exact_cross_venue = (
            delisting_frame.join(
                quote_dates,
                left_on=["symbol", "date"],
                right_on=["symbol", "quote_date"],
                how="inner",
            )
            .filter(pl.col("quote_market") != pl.col("delisting_market"))
            .select(
                "symbol",
                "date",
                pl.col("quote_market").alias("market"),
                "name",
            )
        )
        first_later_quote = (
            delisting_frame.sort(["symbol", "date"])
            .join_asof(
                quote_dates.sort(["symbol", "quote_date"]),
                left_on="date",
                right_on="quote_date",
                by="symbol",
                strategy="forward",
                allow_exact_matches=False,
                check_sortedness=False,
            )
            .drop_nulls("quote_date")
            .select(
                "symbol",
                pl.col("quote_date").alias("date"),
                pl.col("quote_market").alias("market"),
                "name",
            )
        )
        official_listing_evidence = pl.concat(
            [exact_cross_venue, first_later_quote],
            how="vertical_relaxed",
        ).unique(["symbol", "date", "market"], keep="first")
        listing_events.extend(
            {
                **row,
                "source": "receipt_verified_official_daily_ohlcv",
            }
            for row in official_listing_evidence.to_dicts()
        )

    deduped_listings: dict[tuple[str, date, str], dict[str, Any]] = {}
    for event in listing_events:
        key = (event["symbol"], event["date"], event["market"])
        existing = deduped_listings.get(key)
        if existing is None or "company_basic" in event["source"]:
            deduped_listings[key] = event
    listings_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for event in deduped_listings.values():
        listings_by_symbol.setdefault(event["symbol"], []).append(event)
    for events in listings_by_symbol.values():
        events.sort(key=lambda item: (item["date"], item["market"], item["source"]))

    interval_rows: list[dict[str, Any]] = []
    transfer_examples: list[str] = []
    nonterminal_listing_keys: set[tuple[str, date, str]] = set()
    episode_counts: dict[str, int] = {}
    for event in sorted(
        delistings,
        key=lambda item: (item["symbol"], item["date"], item["market"]),
    ):
        symbol_listings = listings_by_symbol.get(event["symbol"], [])
        continuation_dates = {event["date"]}
        if following := following_official_session(event["date"]):
            continuation_dates.add(following)
        continuations = [
            listing
            for listing in symbol_listings
            if listing["market"] != event["market"]
            and listing["date"] in continuation_dates
        ]
        if continuations:
            continuation = continuations[0]
            nonterminal_listing_keys.add(
                (
                    continuation["symbol"],
                    continuation["date"],
                    continuation["market"],
                )
            )
            evidence = (
                "櫃轉市" if event["symbol"] in transfer_symbols else "same_symbol_next_session"
            )
            transfer_examples.append(
                f"{event['date']}:{event['symbol']}:{event['market']}->"
                f"{continuation['market']}:{continuation['date']}:{evidence}"
            )
            continue

        reopen = next(
            (
                listing
                for listing in symbol_listings
                if listing["date"] >= event["date"]
            ),
            None,
        )
        episode_counts[event["symbol"]] = episode_counts.get(event["symbol"], 0) + 1
        interval_rows.append(
            {
                "symbol": event["symbol"],
                "terminal_date": event["date"],
                "terminal_market": event["market"],
                "terminal_name": event["name"],
                "terminal_source": event["source"],
                "reopen_date": reopen["date"] if reopen else None,
                "reopen_market": reopen["market"] if reopen else None,
                "reopen_source": reopen["source"] if reopen else None,
                "episode_id": episode_counts[event["symbol"]],
            }
        )

    intervals = (
        pl.DataFrame(interval_rows, infer_schema_length=None)
        .with_columns(
            pl.col("terminal_date").cast(pl.Date),
            pl.col("reopen_date").cast(pl.Date),
            pl.col("episode_id").cast(pl.Int32),
        )
        .sort(["symbol", "terminal_date"])
        if interval_rows
        else _empty_lifecycle_intervals()
    )
    listing_boundary_rows = [
        {
            "symbol": event["symbol"],
            "date": event["date"],
            "listing_evidence": (
                f"{event['source']}:{event['market']}:{event['date']}"
            ),
        }
        for key, event in sorted(deduped_listings.items())
        if key not in nonterminal_listing_keys
    ]
    listing_boundaries = (
        pl.DataFrame(listing_boundary_rows, infer_schema_length=None)
        .with_columns(pl.col("date").cast(pl.Date))
        .group_by(["symbol", "date"])
        .agg(pl.col("listing_evidence").sort().first())
        .sort(["symbol", "date"])
        if listing_boundary_rows
        else _empty_listing_boundaries()
    )
    return OfficialLifecyclePolicy(
        terminal_intervals=intervals,
        listing_boundaries=listing_boundaries,
        nonterminal_transfer_examples=transfer_examples,
        source_receipts=receipts,
    )


def _annotate_official_lifecycle(
    frame: pl.DataFrame,
    policy: OfficialLifecyclePolicy,
) -> pl.DataFrame:
    empty_interval_columns = (
        pl.lit(None, dtype=pl.Date).alias("terminal_date"),
        pl.lit(None, dtype=pl.String).alias("terminal_market"),
        pl.lit(None, dtype=pl.String).alias("terminal_name"),
        pl.lit(None, dtype=pl.String).alias("terminal_source"),
        pl.lit(None, dtype=pl.Date).alias("reopen_date"),
        pl.lit(None, dtype=pl.String).alias("reopen_market"),
        pl.lit(None, dtype=pl.String).alias("reopen_source"),
    )
    if frame.is_empty():
        return frame.with_columns(
            *empty_interval_columns,
            pl.lit(False).alias("_lifecycle_blocked"),
            pl.lit(0, dtype=pl.Int32).alias("_lifecycle_episode_id"),
            pl.lit("official_lifecycle:no_prior_terminal").alias(
                "_lifecycle_evidence"
            ),
        )
    if policy.terminal_intervals.is_empty():
        return frame.with_columns(
            *empty_interval_columns,
            pl.lit(False).alias("_lifecycle_blocked"),
            pl.lit(0, dtype=pl.Int32).alias("_lifecycle_episode_id"),
            pl.lit("official_lifecycle:no_prior_terminal").alias(
                "_lifecycle_evidence"
            ),
        )
    joined = (
        frame.with_row_index("_lifecycle_row_order")
        .sort(["symbol", "date"])
        .join_asof(
            policy.terminal_intervals,
            left_on="date",
            right_on="terminal_date",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
    )
    blocked = pl.col("terminal_date").is_not_null() & (
        pl.col("reopen_date").is_null() | (pl.col("date") < pl.col("reopen_date"))
    )
    reopened = (
        pl.col("terminal_date").is_not_null()
        & pl.col("reopen_date").is_not_null()
        & (pl.col("date") >= pl.col("reopen_date"))
    )
    return (
        joined.with_columns(
            blocked.alias("_lifecycle_blocked"),
            pl.when(reopened)
            .then(pl.col("episode_id"))
            .otherwise(pl.lit(0, dtype=pl.Int32))
            .alias("_lifecycle_episode_id"),
            pl.when(blocked)
            .then(
                pl.concat_str(
                    pl.lit("official_terminal_delisting:"),
                    pl.col("terminal_source"),
                    pl.lit(":"),
                    pl.col("terminal_date").cast(pl.String),
                )
            )
            .when(reopened)
            .then(
                pl.concat_str(
                    pl.lit("official_relisting:"),
                    pl.col("reopen_source"),
                    pl.lit(":"),
                    pl.col("reopen_date").cast(pl.String),
                )
            )
            .otherwise(pl.lit("official_lifecycle:no_prior_terminal"))
            .alias("_lifecycle_evidence"),
        )
        .sort("_lifecycle_row_order")
        .drop("_lifecycle_row_order", "episode_id")
    )


def _int_field(payload: Any, name: str) -> int:
    if not isinstance(payload, dict):
        return -1
    try:
        return int(payload.get(name, -1))
    except (TypeError, ValueError):
        return -1


def _canonical_yahoo_manifest_code(value: Any) -> str | None:
    code = str(value or "").strip().upper()
    venue: str | None = None
    for suffix in ("_TWO", "_TW"):
        if code.endswith(suffix):
            code, venue = code[: -len(suffix)], suffix[1:]
            break
    if "_" in code or classify_tw_stock_or_etf(code) is None:
        return None
    return code if venue is None else f"{code}_{venue}"


def _yahoo_manifest_codes(path: Path) -> set[str] | None:
    try:
        frame = pl.read_csv(path, infer_schema_length=10000)
        if "code" not in frame.columns:
            return None
        codes: set[str] = set()
        for value in frame.get_column("code").to_list():
            code = _canonical_yahoo_manifest_code(value)
            if code is None or code in codes:
                return None
            codes.add(code)
        return codes
    except Exception:
        return None


def _terminal_unavailable_codes(path: Path) -> set[str] | None:
    accepted_statuses = {"not_found", "delisted_no_history", "delisted_removed"}
    try:
        frame = pl.read_csv(path, infer_schema_length=10000)
        if {"code", "status"} - set(frame.columns):
            return None
        return {
            code
            for row in frame.select("code", "status").iter_rows(named=True)
            if str(row.get("status") or "").strip() in accepted_statuses
            if (code := _canonical_yahoo_manifest_code(row.get("code"))) is not None
        }
    except Exception:
        return None


def _receipt_path_resolver(*search_roots: Path):
    relocation_index: dict[str, list[Path]] | None = None

    def resolve(receipt: Any) -> Path | None:
        nonlocal relocation_index
        if not isinstance(receipt, dict):
            return None
        raw_path = Path(str(receipt.get("path", "")))
        if raw_path.is_file():
            return raw_path
        if not raw_path.name:
            return None
        if relocation_index is None:
            relocation_index = {}
            for root in search_roots:
                if not root.is_dir():
                    continue
                for candidate in root.rglob("*"):
                    if candidate.is_file():
                        relocation_index.setdefault(candidate.name, []).append(candidate)
        candidates = relocation_index.get(raw_path.name, [])
        return candidates[0] if len(candidates) == 1 else None

    return resolve


def _yahoo_input_receipt_is_valid(
    receipt: Any,
    *,
    resolve_path: Any,
    start: date,
    end: date,
) -> bool:
    if not isinstance(receipt, dict):
        return False
    source_path = resolve_path(receipt)
    if source_path is None or not _receipt_matches(source_path, receipt):
        return False
    try:
        metadata = pq.read_schema(source_path).metadata or {}
        decoded: dict[str, str] = {}
        for name, key in YAHOO_INPUT_METADATA.items():
            raw_value = metadata.get(key)
            if not raw_value:
                return False
            decoded[name] = raw_value.decode("utf-8", errors="strict").strip()
        requested_start = date.fromisoformat(decoded["requested_start"])
        checked_through = date.fromisoformat(decoded["checked_through"])
    except (OSError, TypeError, UnicodeDecodeError, ValueError):
        return False
    return (
        bool(str(receipt.get("symbol", "")).strip())
        and receipt.get("venue") in {None, "TW", "TWO"}
        and decoded["source"] == "yahoo"
        and decoded["asset_class"] == "tw_stocks"
        and decoded["source"] == str(receipt.get("source", ""))
        and decoded["asset_class"] == str(receipt.get("asset_class", ""))
        and decoded["requested_start"] == str(receipt.get("requested_start", ""))
        and decoded["checked_through"] == str(receipt.get("checked_through", ""))
        and requested_start <= start
        and checked_through >= end
    )


def _validate_yahoo_fallback_archive(path: Path) -> None:
    summary_path = path.with_suffix(".summary.json")
    input_manifest_path = path.with_suffix(".inputs.json")
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"missing or invalid Yahoo fallback receipt {summary_path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    try:
        input_manifest = json.loads(input_manifest_path.read_text(encoding="utf-8"))
    except Exception:
        input_manifest = None
    source_file_count = _int_field(summary, "source_file_count")
    verified_source_file_count = _int_field(summary, "verified_source_file_count")
    source_symbol_count = _int_field(summary, "source_symbol_count")
    manifest_symbol_count = _int_field(summary, "manifest_symbol_count")
    manifest_record_count = _int_field(summary, "manifest_record_count")
    terminal_unavailable_count = _int_field(
        summary,
        "terminal_unavailable_record_count",
    )
    input_files = (
        input_manifest.get("files")
        if isinstance(input_manifest, dict)
        else None
    )
    terminal_unavailable_codes = (
        input_manifest.get("terminal_unavailable_codes")
        if isinstance(input_manifest, dict)
        else None
    )
    terminal_codes_valid = (
        isinstance(terminal_unavailable_codes, list)
        and all(
            isinstance(code, str)
            and _canonical_yahoo_manifest_code(code) == code
            for code in terminal_unavailable_codes
        )
    )
    terminal_code_set = (
        set(terminal_unavailable_codes)
        if terminal_codes_valid
        else set()
    )
    try:
        start = date.fromisoformat(str(summary.get("start_date")))
        end = date.fromisoformat(str(summary.get("end_date")))
        valid_date_range = date(2000, 1, 1) <= start <= end
    except (AttributeError, TypeError, ValueError):
        start = date.min
        end = date.max
        valid_date_range = False
    raw_input_dir = (
        str(summary.get("input_dir", "")).strip()
        if isinstance(summary, dict)
        else ""
    )
    input_dir = Path(raw_input_dir) if raw_input_dir else path.parent / ".missing-input-dir"
    resolve_path = _receipt_path_resolver(input_dir, path.parent)
    input_files_valid = (
        valid_date_range
        and isinstance(input_files, list)
        and all(
            _yahoo_input_receipt_is_valid(
                receipt,
                resolve_path=resolve_path,
                start=start,
                end=end,
            )
            for receipt in input_files
        )
    )
    input_symbols = (
        {str(receipt.get("symbol", "")).strip() for receipt in input_files}
        if isinstance(input_files, list)
        and all(isinstance(receipt, dict) for receipt in input_files)
        else set()
    )
    input_codes = (
        {
            (
                str(receipt.get("symbol", "")).strip()
                if receipt.get("venue") is None
                else f"{str(receipt.get('symbol', '')).strip()}_{receipt.get('venue')}"
            )
            for receipt in input_files
        }
        if isinstance(input_files, list)
        and all(isinstance(receipt, dict) for receipt in input_files)
        else set()
    )
    input_paths = (
        [str(receipt.get("path", "")) for receipt in input_files]
        if isinstance(input_files, list)
        and all(isinstance(receipt, dict) for receipt in input_files)
        else []
    )
    symbol_manifest_receipt = (
        input_manifest.get("symbol_manifest_receipt")
        if isinstance(input_manifest, dict)
        else None
    )
    symbol_manifest_path = resolve_path(symbol_manifest_receipt)
    manifest_codes = (
        _yahoo_manifest_codes(symbol_manifest_path)
        if symbol_manifest_path is not None
        else None
    )
    terminal_report_receipt = (
        input_manifest.get("terminal_report_receipt")
        if isinstance(input_manifest, dict)
        else None
    )
    terminal_report_path = resolve_path(terminal_report_receipt)
    terminal_report_codes = (
        _terminal_unavailable_codes(terminal_report_path)
        if terminal_report_path is not None
        else None
    )
    try:
        archive_schema_valid = {
            "date",
            "symbol",
            "market",
            "source_factor",
            "quote_source",
        } <= set(pq.read_schema(path).names)
    except (OSError, TypeError, ValueError):
        archive_schema_valid = False
    checks = {
        "summary_object": isinstance(summary, dict),
        "summary_schema": _int_field(summary, "schema_version") >= 2,
        "source": isinstance(summary, dict)
        and summary.get("source") == YAHOO_FALLBACK_SOURCE_NAME,
        "failed_symbols": isinstance(summary, dict)
        and _int_field(summary, "failed_symbol_count") == 0,
        "coverage_receipts_complete": isinstance(summary, dict)
        and summary.get("coverage_receipts_complete") is True,
        "symbol_accounting_complete": isinstance(summary, dict)
        and summary.get("symbol_accounting_complete") is True,
        "output_receipt": isinstance(summary, dict)
        and _receipt_matches(path, summary.get("output_receipt")),
        "archive_schema": archive_schema_valid,
        "date_range": valid_date_range,
        "source_counts": (
            source_file_count >= 0
            and source_file_count == verified_source_file_count
            and source_symbol_count >= 0
            and _int_field(summary, "ok_symbol_count")
            + _int_field(summary, "empty_symbol_count")
            == source_symbol_count
        ),
        "input_manifest_receipt": isinstance(summary, dict)
        and _receipt_matches(
            input_manifest_path,
            summary.get("input_manifest_receipt"),
        ),
        "input_manifest_header": (
            isinstance(input_manifest, dict)
            and _int_field(input_manifest, "schema_version") >= 1
            and input_manifest.get("source") == "yahoo"
            and str(input_manifest.get("start_date")) == start.isoformat()
            and str(input_manifest.get("end_date")) == end.isoformat()
            and Path(str(summary.get("input_manifest_path", ""))).name
            == input_manifest_path.name
        ),
        "input_manifest_counts": (
            isinstance(input_files, list)
            and source_file_count == len(input_files)
            and source_file_count == _int_field(input_manifest, "file_count")
            and source_symbol_count == len(input_symbols)
            and manifest_symbol_count >= 0
            and manifest_symbol_count
            == _int_field(input_manifest, "manifest_symbol_count")
            and manifest_record_count >= 0
            and manifest_record_count
            == _int_field(input_manifest, "manifest_record_count")
            and terminal_codes_valid
            and terminal_unavailable_count >= 0
            and terminal_unavailable_count
            == len(terminal_code_set)
            and len(terminal_unavailable_codes) == len(terminal_code_set)
        ),
        "manifest_record_accounting": (
            manifest_codes is not None
            and manifest_record_count == len(manifest_codes)
            and manifest_symbol_count
            == len(
                {
                    code.removesuffix("_TW").removesuffix("_TWO")
                    for code in manifest_codes
                }
            )
            and input_codes <= manifest_codes
            and terminal_code_set <= manifest_codes
            and manifest_codes == input_codes | terminal_code_set
        ),
        "input_manifest_files": input_files_valid
        and len(input_paths) == len(set(input_paths)),
        "symbol_manifest_receipt": symbol_manifest_path is not None
        and _receipt_matches(symbol_manifest_path, symbol_manifest_receipt),
        "terminal_report_receipt": (
            terminal_unavailable_count == 0
            and terminal_report_receipt is None
        )
        or (
            terminal_unavailable_count > 0
            and terminal_report_path is not None
            and _receipt_matches(terminal_report_path, terminal_report_receipt)
            and terminal_report_codes is not None
            and terminal_code_set == terminal_report_codes & (manifest_codes or set())
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(
            f"Yahoo fallback receipt failed checks {failed}: {summary_path}"
        )


def _fsync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_official_quote_parquet(
    frame: pl.DataFrame,
    output_path: Path,
    *,
    checked_through: str,
    source: str = OFFICIAL_SOURCE_NAME,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    first_date = str(frame["date"].min())
    last_date = str(frame["date"].max())
    table = frame.to_arrow()
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            PARQUET_METADATA["source"]: str(source).encode("utf-8"),
            PARQUET_METADATA["asset_class"]: b"tw_stocks",
            PARQUET_METADATA["checked_through"]: str(checked_through).encode("utf-8"),
            PARQUET_METADATA["first_date"]: first_date.encode("utf-8"),
            PARQUET_METADATA["last_date"]: last_date.encode("utf-8"),
            PARQUET_METADATA["write_ts_utc"]: (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat().encode("utf-8")
            ),
        }
    )
    table = table.replace_schema_metadata(metadata)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        pq.write_table(
            table,
            temporary,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
            row_group_size=128_000,
        )
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, output_path)
        _fsync_directory(output_path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _update_return_price_provenance(
    root: Path,
    symbols: set[str],
    *,
    fallback_symbols: set[str] | None = None,
    dry_run: bool,
) -> None:
    path = root / "return_price_provenance.json"
    payload: dict[str, Any] = {"schema_version": 1, "symbols": {}}
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    fallback_symbols = set(fallback_symbols or set())
    for symbol in sorted(symbols):
        uses_fallback = symbol in fallback_symbols
        payload["symbols"][symbol] = {
            "kind": (
                "official_reference_index_10_with_yahoo_fallback"
                if uses_fallback
                else "official_reference_index_10"
            ),
            "source": MIXED_FALLBACK_SOURCE_NAME if uses_fallback else OFFICIAL_SOURCE_NAME,
            "updated_at_utc": updated_at,
        }
    if dry_run:
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _remove_stale_official_symbol_files(
    output_dir: Path,
    target_symbols: set[str],
    *,
    dry_run: bool,
) -> list[str]:
    removed: list[str] = []
    for path in sorted(output_dir.glob("*_features.parquet")):
        symbol = path.name.removesuffix("_features.parquet")
        if symbol in target_symbols:
            continue
        metadata = pq.read_schema(path).metadata or {}
        source = metadata.get(PARQUET_METADATA["source"], b"").decode("utf-8", errors="replace")
        if source not in {OFFICIAL_SOURCE_NAME, MIXED_FALLBACK_SOURCE_NAME}:
            raise RuntimeError(f"refusing to remove non-official stale symbol file: {path}")
        removed.append(symbol)
        if not dry_run:
            path.unlink()
    if removed and not dry_run:
        _fsync_directory(output_dir)
    return removed


def _signed_number_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8, strict=False)
        .str.replace_all(r"[\s,]", "")
        .str.replace_all("−", "-")
        .str.replace_all("＋", "+")
        .cast(pl.Float64, strict=False)
    )


def _twse_frame(path: Path) -> pl.DataFrame:
    schema = set(pl.read_parquet_schema(path))
    required = {
        "date",
        "證券代號",
        "證券名稱",
        "開盤價",
        "最高價",
        "最低價",
        "收盤價",
        "成交股數",
        "漲跌(+/-)",
        "漲跌價差",
    }
    missing = sorted(required - schema)
    if missing:
        raise ValueError(f"{path} is missing official TWSE columns: {missing}")
    direction = pl.col("漲跌(+/-)").cast(pl.Utf8, strict=False).str.strip_chars()
    magnitude = _num_expr("漲跌價差").abs()
    signed_change = (
        pl.when(direction == "-")
        .then(-magnitude)
        .when(direction == "+")
        .then(magnitude)
        .when(magnitude.fill_null(0.0) == 0.0)
        .then(0.0)
        .otherwise(None)
    )
    return (
        pl.read_parquet(path)
        .select(
            _date_column_expr("date").alias("date"),
            _symbol_expr("證券代號").str.to_uppercase().alias("symbol"),
            pl.col("證券名稱").cast(pl.Utf8, strict=False).str.strip_chars().alias("name"),
            pl.lit("twse").alias("market"),
            _num_expr("開盤價").alias("open"),
            _num_expr("最高價").alias("max"),
            _num_expr("最低價").alias("min"),
            _num_expr("收盤價").alias("close"),
            _num_expr("成交股數").alias("Trading_Volume"),
            signed_change.alias("signed_change"),
            pl.lit(None, dtype=pl.Float64).alias("source_next_reference"),
        )
    )


def _tpex_frame(path: Path) -> pl.DataFrame:
    schema = set(pl.read_parquet_schema(path))
    required = {"date", "代號", "名稱", "開盤", "最高", "最低", "收盤", "成交股數", "漲跌"}
    missing = sorted(required - schema)
    if missing:
        raise ValueError(f"{path} is missing official TPEx columns: {missing}")
    change_text = pl.col("漲跌").cast(pl.Utf8, strict=False).str.strip_chars()
    next_reference = (
        _num_expr("次日參考價")
        if "次日參考價" in schema
        else pl.lit(None, dtype=pl.Float64)
    )
    return (
        pl.read_parquet(path)
        .select(
            _date_column_expr("date").alias("date"),
            _symbol_expr("代號").str.to_uppercase().alias("symbol"),
            pl.col("名稱").cast(pl.Utf8, strict=False).str.strip_chars().alias("name"),
            pl.lit("tpex").alias("market"),
            _num_expr("開盤").alias("open"),
            _num_expr("最高").alias("max"),
            _num_expr("最低").alias("min"),
            _num_expr("收盤").alias("close"),
            _num_expr("成交股數").alias("Trading_Volume"),
            pl.when(change_text.is_in(["", "---", "--", "-"]))
            .then(None)
            .otherwise(_signed_number_expr("漲跌"))
            .alias("signed_change"),
            next_reference.alias("source_next_reference"),
        )
    )


def _legacy_official_frame(
    path: Path,
    *,
    default_quote_source: str = "official_archive",
) -> pl.DataFrame:
    if path.suffix.lower() in {".parquet", ".pq"}:
        frame = pl.read_parquet(path)
    elif path.suffix.lower() in {".csv", ".txt"}:
        frame = pl.read_csv(path, infer_schema_length=0)
    else:
        raise ValueError(f"unsupported official archive format: {path}")
    rename = {
        source: target
        for source, target in LEGACY_COLUMN_ALIASES.items()
        if source in frame.columns and target not in frame.columns
    }
    if rename:
        frame = frame.rename(rename)
    required = {"date", "symbol", "market", "open", "max", "min", "close", "Trading_Volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{path} is missing official archive columns: {missing}")
    name = (
        pl.col("name").cast(pl.Utf8, strict=False).str.strip_chars()
        if "name" in frame.columns
        else pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars()
    )
    signed_change = (
        pl.col("signed_change").cast(pl.Float64, strict=False).fill_nan(None)
        if "signed_change" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    source_reference = (
        pl.col("source_reference").cast(pl.Float64, strict=False).fill_nan(None)
        if "source_reference" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    source_adjclose = (
        pl.col("source_adjclose").cast(pl.Float64, strict=False).fill_nan(None)
        if "source_adjclose" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    source_factor = (
        pl.col("source_factor").cast(pl.Float64, strict=False).fill_nan(None)
        if "source_factor" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    quote_source = (
        pl.col("quote_source").cast(pl.Utf8, strict=False).str.strip_chars()
        if "quote_source" in frame.columns
        else pl.lit(default_quote_source)
    )
    normalized_market = (
        pl.col("market")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.to_lowercase()
        .replace(
            {
                "listed": "twse",
                "tse": "twse",
                "otc": "tpex",
                "gtsm": "tpex",
            }
        )
    )
    normalized = frame.select(
        _date_column_expr("date").alias("date"),
        pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase(),
        name.alias("name"),
        normalized_market.alias("market"),
        *[
            pl.col(column).cast(pl.Float64, strict=False).fill_nan(None).alias(column)
            for column in ("open", "max", "min", "close", "Trading_Volume")
        ],
        signed_change.alias("signed_change"),
        source_reference.alias("source_reference"),
        source_adjclose.alias("source_adjclose"),
        source_factor.alias("source_factor"),
        pl.lit(None, dtype=pl.Float64).alias("source_next_reference"),
        quote_source.alias("quote_source"),
    )
    invalid_market = normalized.filter(~pl.col("market").is_in(["twse", "tpex"]))
    if invalid_market.height:
        raise ValueError(f"{path} has {invalid_market.height} rows outside TWSE/TPEx")
    duplicate_keys = (
        normalized.group_by(["date", "symbol"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    if duplicate_keys:
        raise ValueError(f"{path} has {duplicate_keys} duplicate date-symbol keys")
    return normalized


def _official_frame(
    input_dir: Path,
    legacy_paths: list[Path] | None = None,
    fallback_paths: list[Path] | None = None,
    end_date: date | None = None,
) -> tuple[
    pl.DataFrame,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    OfficialMergeStats,
]:
    core_paths = [input_dir / name for name in OFFICIAL_CORE_SOURCE_FILENAMES]
    missing = [str(path) for path in core_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"official daily OHLCV sources are missing: {missing}")
    legacy_paths = list(legacy_paths or [])
    fallback_paths = list(fallback_paths or [])
    missing_legacy = [str(path) for path in legacy_paths if not path.is_file()]
    if missing_legacy:
        raise FileNotFoundError(f"official legacy archives are missing: {missing_legacy}")
    missing_fallback = [str(path) for path in fallback_paths if not path.is_file()]
    if missing_fallback:
        raise FileNotFoundError(f"OHLCV fallback archives are missing: {missing_fallback}")
    lifecycle_paths = [input_dir / name for name in LIFECYCLE_EVIDENCE_FILENAMES]
    if fallback_paths:
        missing_lifecycle = [str(path) for path in lifecycle_paths if not path.is_file()]
        if missing_lifecycle:
            raise FileNotFoundError(
                "Yahoo fallback requires complete official lifecycle evidence; "
                f"missing={missing_lifecycle}"
            )
    paths = _official_symbol_source_paths(input_dir)
    (
        session_calendar,
        session_calendar_receipt,
        session_calendar_summary_receipt,
    ) = _load_verified_taiex_session_calendar(input_dir)
    before = [_file_content_receipt(path) for path in paths]
    legacy_before = [_receipt(path) for path in legacy_paths]
    fallback_before = [_receipt(path) for path in fallback_paths]
    merge_columns = [
        "date",
        "symbol",
        "name",
        "market",
        "open",
        "max",
        "min",
        "close",
        "Trading_Volume",
        "signed_change",
        "source_reference",
        "source_adjclose",
        "source_factor",
        "source_next_reference",
        "quote_source",
        "_legacy_source_id",
        "_source_priority",
    ]
    current = pl.concat(
        [
            _twse_frame(core_paths[0]).with_columns(
                pl.lit("twse_official").alias("quote_source")
            ),
            _tpex_frame(core_paths[1]).with_columns(
                pl.lit("tpex_official").alias("quote_source")
            ),
        ],
        how="vertical_relaxed",
    ).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("source_reference"),
        pl.lit(None, dtype=pl.Float64).alias("source_adjclose"),
        pl.lit(None, dtype=pl.Float64).alias("source_factor"),
        pl.lit(-1, dtype=pl.Int32).alias("_legacy_source_id"),
        pl.lit(2, dtype=pl.Int8).alias("_source_priority"),
    ).select(merge_columns)
    if end_date is not None:
        current = current.filter(pl.col("date") <= pl.lit(end_date))
    lifecycle_policy = _load_official_lifecycle_policy(
        input_dir,
        current,
        session_calendar,
        required=bool(fallback_paths),
    )
    legacy_frames = [
        _legacy_official_frame(path, default_quote_source="official_archive").with_columns(
            pl.lit(source_id, dtype=pl.Int32).alias("_legacy_source_id"),
            pl.lit(1, dtype=pl.Int8).alias("_source_priority"),
        ).select(merge_columns)
        for source_id, path in enumerate(legacy_paths)
    ]
    fallback_frames = [
        _legacy_official_frame(
            path,
            default_quote_source=YAHOO_FALLBACK_SOURCE_NAME,
        ).with_columns(
            pl.lit(YAHOO_FALLBACK_SOURCE_NAME).alias("quote_source"),
            pl.lit(len(legacy_frames) + source_id, dtype=pl.Int32).alias(
                "_legacy_source_id"
            ),
            pl.lit(0, dtype=pl.Int8).alias("_source_priority"),
        ).select(merge_columns)
        for source_id, path in enumerate(fallback_paths)
    ]
    if end_date is not None:
        cutoff = pl.lit(end_date)
        legacy_frames = [
            frame.filter(pl.col("date") <= cutoff) for frame in legacy_frames
        ]
        fallback_frames = [
            frame.filter(pl.col("date") <= cutoff) for frame in fallback_frames
        ]
    for path, fallback_frame in zip(fallback_paths, fallback_frames, strict=True):
        before_2000 = fallback_frame.filter(pl.col("date") < pl.lit(date(2000, 1, 1)))
        if before_2000.height:
            raise ValueError(
                f"{path} has {before_2000.height} fallback rows before 2000-01-01"
            )

    if fallback_frames:
        off_calendar_fallback = (
            pl.concat(fallback_frames, how="vertical_relaxed")
            .join(session_calendar, on="date", how="anti")
            .select("date", "symbol")
            .sort(["date", "symbol"])
        )
        fallback_frames = [
            fallback_frame.join(session_calendar, on="date", how="semi")
            for fallback_frame in fallback_frames
        ]
    else:
        off_calendar_fallback = pl.DataFrame(
            schema={"date": pl.Date, "symbol": pl.String}
        )
    dropped_off_calendar_fallback_rows = int(off_calendar_fallback.height)
    dropped_off_calendar_fallback_examples = [
        f"{row['date']}:{row['symbol']}"
        for row in off_calendar_fallback.head(20).to_dicts()
    ]
    fallback_rows_before_lifecycle_filter = sum(
        fallback_frame.height for fallback_frame in fallback_frames
    )
    lifecycle_dropped_frames: list[pl.DataFrame] = []
    lifecycle_filtered_frames: list[pl.DataFrame] = []
    for fallback_frame in fallback_frames:
        annotated = _annotate_official_lifecycle(
            fallback_frame,
            lifecycle_policy,
        )
        lifecycle_dropped_frames.append(
            annotated.filter(pl.col("_lifecycle_blocked")).select(
                "date",
                "symbol",
                "terminal_date",
                "terminal_source",
            )
        )
        lifecycle_filtered_frames.append(
            annotated.filter(~pl.col("_lifecycle_blocked")).select(merge_columns)
        )
    fallback_frames = lifecycle_filtered_frames
    fallback_rows_after_lifecycle_filter = sum(
        fallback_frame.height for fallback_frame in fallback_frames
    )
    if lifecycle_dropped_frames:
        dropped_post_terminal_fallback = pl.concat(
            lifecycle_dropped_frames,
            how="vertical_relaxed",
        ).sort(["date", "symbol"])
    else:
        dropped_post_terminal_fallback = pl.DataFrame(
            schema={
                "date": pl.Date,
                "symbol": pl.String,
                "terminal_date": pl.Date,
                "terminal_source": pl.String,
            }
        )
    dropped_post_terminal_fallback_rows = int(
        dropped_post_terminal_fallback.height
    )
    if (
        fallback_rows_before_lifecycle_filter
        != fallback_rows_after_lifecycle_filter
        + dropped_post_terminal_fallback_rows
    ):
        raise RuntimeError("Yahoo lifecycle filter row reconciliation failed")
    dropped_post_terminal_fallback_examples = [
        f"{row['date']}:{row['symbol']}:terminal={row['terminal_date']}:"
        f"source={row['terminal_source']}"
        for row in dropped_post_terminal_fallback.head(20).to_dicts()
    ]

    def reject_overlaps(frames: list[pl.DataFrame], label: str) -> None:
        if len(frames) <= 1:
            return
        duplicate = (
            pl.concat(frames, how="vertical_relaxed")
            .group_by(["date", "symbol"])
            .len()
            .filter(pl.col("len") > 1)
        )
        if duplicate.height:
            raise ValueError(
                f"{label} archives overlap on {duplicate.height} date-symbol keys"
            )

    reject_overlaps(legacy_frames, "legacy official")
    reject_overlaps(fallback_frames, "fallback")
    frame = pl.concat(
        [*fallback_frames, *legacy_frames, current],
        how="vertical_relaxed",
    )
    if fallback_frames:
        fallback_adjustments = (
            pl.concat(fallback_frames, how="vertical_relaxed")
            .select(
                "date",
                "symbol",
                pl.col("source_factor").alias("_yahoo_fallback_factor"),
            )
            .unique(["date", "symbol"], keep="last", maintain_order=True)
        )
        frame = frame.join(fallback_adjustments, on=["date", "symbol"], how="left")
    else:
        frame = frame.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("_yahoo_fallback_factor")
        )
    reference = (
        pl.read_parquet(core_paths[2])
        .select(
            _date_column_expr("date").alias("date"),
            pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase(),
            pl.coalesce(
                pl.col("reference_price").cast(pl.Float64, strict=False),
                pl.col("opening_reference_price").cast(pl.Float64, strict=False),
            ).alias("reference_override"),
        )
        .drop_nulls(["date", "symbol", "reference_override"])
        .unique(["date", "symbol"], keep="last", maintain_order=True)
    )
    frame = frame.join(reference, on=["date", "symbol"], how="left").with_columns(
        pl.coalesce("reference_override", "source_reference").alias("reference_override")
    )
    after = [_file_content_receipt(path) for path in paths]
    legacy_after = [_receipt(path) for path in legacy_paths]
    fallback_after = [_receipt(path) for path in fallback_paths]
    if (
        before != after
        or legacy_before != legacy_after
        or fallback_before != fallback_after
        or session_calendar_receipt
        != _file_content_receipt(input_dir / "twse_taiex_ohlc.parquet")
        or session_calendar_summary_receipt
        != _file_content_receipt(input_dir / "twse_taiex_ohlc.summary.json")
    ):
        raise RuntimeError("official OHLCV source changed during symbol build")
    stock_symbol = pl.col("symbol").str.contains(TW_STOCK_SYMBOL_PATTERN).fill_null(False)
    etf_symbol = pl.col("symbol").str.contains(TW_ETF_SYMBOL_PATTERN).fill_null(False)
    in_scope = (
        (stock_symbol | etf_symbol)
        & pl.col("date").is_not_null()
        & (pl.col("date") >= pl.lit(date(2000, 1, 1)))
    )
    official_row = pl.col("quote_source") != YAHOO_FALLBACK_SOURCE_NAME
    positive_prices = pl.all_horizontal(
        *[
            pl.col(column).is_not_null()
            & pl.col(column).is_finite()
            & (pl.col(column) > 0.0)
            for column in ("open", "max", "min", "close")
        ]
    )
    valid_geometry = (
        (pl.col("max") >= pl.max_horizontal("open", "min", "close"))
        & (pl.col("min") <= pl.min_horizontal("open", "max", "close"))
    )
    standard_usable_bar = positive_prices & valid_geometry
    official_positive_close_sentinel = (
        official_row
        & (pl.col("open") == 0.0)
        & (pl.col("max") == 0.0)
        & (pl.col("min") == 0.0)
        & pl.col("close").is_not_null()
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
    )
    usable_official_bar = standard_usable_bar | official_positive_close_sentinel
    official_unusable_keys = (
        frame.filter(in_scope & official_row & ~usable_official_bar.fill_null(False))
        .select("date", "symbol")
        .unique()
    )
    usable_official_keys = (
        frame.filter(in_scope & official_row & usable_official_bar.fill_null(False))
        .select("date", "symbol")
        .unique()
    )
    needs_fallback_keys = official_unusable_keys.join(
        usable_official_keys,
        on=["date", "symbol"],
        how="anti",
    )
    usable_fallback_keys = (
        frame.filter(
            in_scope
            & (pl.col("quote_source") == YAHOO_FALLBACK_SOURCE_NAME)
            & standard_usable_bar.fill_null(False)
        )
        .select("date", "symbol")
        .unique()
    )
    replaced_unusable_keys = needs_fallback_keys.join(
        usable_fallback_keys,
        on=["date", "symbol"],
        how="inner",
    )
    unfilled_unusable_keys = needs_fallback_keys.join(
        usable_fallback_keys,
        on=["date", "symbol"],
        how="anti",
    )
    merge_stats = OfficialMergeStats(
        official_unusable_ohlcv_rows=needs_fallback_keys.height,
        fallback_replaced_unusable_official_rows=replaced_unusable_keys.height,
        unfilled_unusable_official_rows=unfilled_unusable_keys.height,
        unfilled_unusable_official_examples=[
            f"{row['date']}:{row['symbol']}"
            for row in unfilled_unusable_keys.sort(["date", "symbol"])
            .head(20)
            .to_dicts()
        ],
        session_calendar_rows=int(session_calendar.height),
        session_calendar_receipt=session_calendar_receipt,
        session_calendar_summary_receipt=session_calendar_summary_receipt,
        dropped_off_calendar_fallback_rows=dropped_off_calendar_fallback_rows,
        dropped_off_calendar_fallback_examples=(
            dropped_off_calendar_fallback_examples
        ),
        fallback_rows_before_lifecycle_filter=(
            fallback_rows_before_lifecycle_filter
        ),
        fallback_rows_after_lifecycle_filter=(
            fallback_rows_after_lifecycle_filter
        ),
        dropped_post_terminal_fallback_rows=(
            dropped_post_terminal_fallback_rows
        ),
        dropped_post_terminal_fallback_examples=(
            dropped_post_terminal_fallback_examples
        ),
        lifecycle_terminal_events=int(
            lifecycle_policy.terminal_intervals.height
        ),
        lifecycle_nonterminal_transfer_events=len(
            lifecycle_policy.nonterminal_transfer_examples
        ),
        lifecycle_nonterminal_transfer_examples=(
            lifecycle_policy.nonterminal_transfer_examples[:20]
        ),
        lifecycle_source_receipts=lifecycle_policy.source_receipts,
    )
    unusable_marker = needs_fallback_keys.with_columns(
        pl.lit(True).alias("_official_ohlcv_unusable")
    )
    frame = frame.join(
        unusable_marker,
        on=["date", "symbol"],
        how="left",
    ).with_columns(
        pl.col("_official_ohlcv_unusable").fill_null(False)
    )
    frame = (
        frame.with_columns(
            pl.when(stock_symbol)
            .then(pl.lit("stock"))
            .when(etf_symbol)
            .then(pl.lit("etf"))
            .otherwise(None)
            .alias("security_type")
        )
        .drop_nulls("security_type")
        .filter(pl.col("date") >= pl.lit(date(2000, 1, 1)))
        .filter(
            (
                official_row
                & usable_official_bar.fill_null(False)
            )
            | (
                ~official_row
                & standard_usable_bar.fill_null(False)
            )
        )
        .sort(["symbol", "date", "_source_priority", "market"])
        .unique(["symbol", "date"], keep="last", maintain_order=True)
        .with_columns(
            pl.when(
                (pl.col("quote_source") == YAHOO_FALLBACK_SOURCE_NAME)
                & pl.col("_official_ohlcv_unusable")
            )
            .then(pl.lit("official_ohlcv_unusable"))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("fallback_reason")
        )
    )
    frame = _annotate_official_lifecycle(frame, lifecycle_policy)
    if lifecycle_policy.listing_boundaries.is_empty():
        frame = frame.with_columns(
            pl.lit(False).alias("_official_listing_boundary"),
            pl.lit(None, dtype=pl.String).alias("_official_listing_evidence"),
        )
    else:
        frame = (
            frame.join(
                lifecycle_policy.listing_boundaries,
                on=["symbol", "date"],
                how="left",
            )
            .with_columns(
                pl.col("listing_evidence")
                .is_not_null()
                .alias("_official_listing_boundary"),
                pl.col("listing_evidence").alias("_official_listing_evidence"),
            )
            .drop("listing_evidence")
        )
    unexpected_official_post_terminal = frame.filter(
        pl.col("_lifecycle_blocked")
        & (pl.col("quote_source") != YAHOO_FALLBACK_SOURCE_NAME)
    )
    if unexpected_official_post_terminal.height:
        examples = (
            unexpected_official_post_terminal.select(
                "date",
                "symbol",
                "quote_source",
                "terminal_date",
            )
            .head(10)
            .to_dicts()
        )
        raise RuntimeError(
            "official quote rows occur after a terminal delisting without "
            f"verified relisting evidence: examples={examples}"
        )
    frame = frame.filter(~pl.col("_lifecycle_blocked"))

    session_links = session_calendar.sort("date").with_columns(
        pl.col("date").shift(1).alias("_previous_official_session")
    )
    previous_next_references = (
        current.filter(
            (pl.col("quote_source") == "tpex_official")
            & pl.col("source_next_reference").is_not_null()
            & pl.col("source_next_reference").is_finite()
            & (pl.col("source_next_reference") > 0.0)
        )
        .select(
            "symbol",
            pl.col("date").alias("_previous_official_session"),
            pl.col("source_next_reference").alias(
                "_previous_session_next_reference"
            ),
        )
    )
    duplicate_previous_references = (
        previous_next_references.group_by(
            ["symbol", "_previous_official_session"]
        )
        .len()
        .filter(pl.col("len") > 1)
    )
    if duplicate_previous_references.height:
        raise RuntimeError(
            "official TPEx previous-session next-reference keys are duplicated"
        )
    frame = (
        frame.join(session_links, on="date", how="left")
        .join(
            previous_next_references,
            on=["symbol", "_previous_official_session"],
            how="left",
        )
        .sort(["symbol", "date"])
        .with_columns(
            pl.col("date").shift(1).over("symbol").alias(
                "_selected_previous_date"
            ),
            pl.col("_lifecycle_episode_id").shift(1).over("symbol").alias(
                "_selected_previous_episode"
            ),
        )
    )
    previous_reference_candidate = (
        (pl.col("quote_source") == "tpex_official")
        & pl.col("_previous_official_session").is_not_null()
        & (pl.col("_selected_previous_date") == pl.col("_previous_official_session"))
        & (
            pl.col("_selected_previous_episode")
            == pl.col("_lifecycle_episode_id")
        )
        & pl.col("_previous_session_next_reference").is_not_null()
        & pl.col("_previous_session_next_reference").is_finite()
        & (pl.col("_previous_session_next_reference") > 0.0)
        & pl.col("close").is_not_null()
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
        & pl.col("source_factor").is_null()
        & pl.col("source_adjclose").is_null()
        & pl.col("reference_override").is_null()
        & pl.col("signed_change").is_null()
        & pl.col("Trading_Volume").is_not_null()
        & pl.col("Trading_Volume").is_finite()
        & (pl.col("Trading_Volume") > 0.0)
    )
    frame = frame.with_columns(
        previous_reference_candidate.alias(
            "_previous_session_next_reference_eligible"
        )
    )
    previous_reference_candidates = frame.filter(
        pl.col("_previous_session_next_reference_eligible")
    )
    merge_stats.previous_session_next_reference_candidate_rows = int(
        previous_reference_candidates.height
    )
    merge_stats.previous_session_next_reference_candidate_examples = [
        f"{row['date']}:{row['symbol']}:previous_session="
        f"{row['_previous_official_session']}:reference="
        f"{row['_previous_session_next_reference']}"
        for row in previous_reference_candidates.select(
            "date",
            "symbol",
            "_previous_official_session",
            "_previous_session_next_reference",
        )
        .head(20)
        .to_dicts()
    ]
    # Some legacy official TPEx reports use the exact sentinel O=H=L=0 while
    # still publishing a positive official close and volume.  Keep the raw
    # archive untouched, but make the derived per-symbol K-line a flat bar at
    # that same official close.  This does not borrow or overwrite the quote
    # with Yahoo data, and the row-level normalization remains explicit below.
    official_zero_ohlc = (
        (pl.col("quote_source") != YAHOO_FALLBACK_SOURCE_NAME)
        & (pl.col("open") == 0.0)
        & (pl.col("max") == 0.0)
        & (pl.col("min") == 0.0)
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
    )
    frame = frame.with_columns(
        official_zero_ohlc.alias("_official_zero_ohlc_normalized")
    ).with_columns(
        [
            pl.when(pl.col("_official_zero_ohlc_normalized"))
            .then(pl.col("close"))
            .otherwise(pl.col(column))
            .alias(column)
            for column in ("open", "max", "min")
        ]
    )
    if frame.is_empty():
        raise RuntimeError("official TWSE/TPEx sources produced no valid stock rows")
    return frame, before, legacy_before, fallback_before, merge_stats


def _normalized_reference_index(
    close: np.ndarray,
    signed_change: np.ndarray,
    volume: np.ndarray,
    reference_override: np.ndarray | None = None,
    factor_override: np.ndarray | None = None,
    episode_ids: np.ndarray | None = None,
) -> tuple[np.ndarray, int]:
    close = np.asarray(close, dtype=np.float64)
    change = np.asarray(signed_change, dtype=np.float64)
    volume = np.asarray(volume, dtype=np.float64)
    reference_override = (
        np.asarray(reference_override, dtype=np.float64)
        if reference_override is not None
        else np.full(close.shape, np.nan, dtype=np.float64)
    )
    factor_override = (
        np.asarray(factor_override, dtype=np.float64)
        if factor_override is not None
        else np.full(close.shape, np.nan, dtype=np.float64)
    )
    episodes = (
        np.asarray(episode_ids)
        if episode_ids is not None
        else np.zeros(close.shape, dtype=np.int64)
    )
    if episodes.shape != close.shape:
        raise ValueError("episode_ids must have the same shape as close")
    output = np.full(close.shape, np.nan, dtype=np.float64)
    if close.size == 0:
        return output, 0
    state = 10.0
    output[0] = state
    missing = 0
    for idx in range(1, close.size):
        if episodes[idx] != episodes[idx - 1]:
            state = 10.0
            output[idx] = state
            continue
        if not math.isfinite(close[idx]) or close[idx] <= 0:
            missing += 1
            continue
        if math.isfinite(factor_override[idx]) and factor_override[idx] > 0:
            factor = factor_override[idx]
        elif math.isfinite(reference_override[idx]) and reference_override[idx] > 0:
            factor = close[idx] / reference_override[idx]
        elif math.isfinite(change[idx]):
            reference = close[idx] - change[idx]
            factor = close[idx] / reference if reference > 0 else math.nan
        elif math.isfinite(volume[idx]) and volume[idx] == 0:
            factor = 1.0
        else:
            factor = math.nan
        if not math.isfinite(factor) or factor <= 0:
            missing += 1
            continue
        state *= factor
        output[idx] = state
    return output, missing


def _source_adjustment_factors(
    source_adjclose: np.ndarray,
    source_ids: np.ndarray | None = None,
    episode_ids: np.ndarray | None = None,
) -> np.ndarray:
    values = np.asarray(source_adjclose, dtype=np.float64)
    groups = (
        np.asarray(source_ids)
        if source_ids is not None
        else np.zeros(values.shape, dtype=np.int64)
    )
    if groups.shape != values.shape:
        raise ValueError("source_ids must have the same shape as source_adjclose")
    episodes = (
        np.asarray(episode_ids)
        if episode_ids is not None
        else np.zeros(values.shape, dtype=np.int64)
    )
    if episodes.shape != values.shape:
        raise ValueError("episode_ids must have the same shape as source_adjclose")
    factors = np.full(values.shape, np.nan, dtype=np.float64)
    if values.size < 2:
        return factors
    valid = (
        np.isfinite(values[1:])
        & np.isfinite(values[:-1])
        & (values[1:] > 0)
        & (values[:-1] > 0)
        & (groups[1:] == groups[:-1])
        & (episodes[1:] == episodes[:-1])
    )
    factors[1:] = np.where(valid, values[1:] / values[:-1], np.nan)
    return factors


def _write_symbol(
    output_dir: Path,
    symbol: str,
    frame: pl.DataFrame,
    *,
    requested_end_date: str,
    dry_run: bool,
) -> BuildResult:
    path = output_dir / f"{symbol}_features.parquet"
    try:
        frame = frame.sort("date")
        if "_official_zero_ohlc_normalized" not in frame.columns:
            frame = frame.with_columns(
                pl.lit(False).alias("_official_zero_ohlc_normalized")
            )
        if "fallback_reason" not in frame.columns:
            frame = frame.with_columns(
                pl.lit(None, dtype=pl.String).alias("fallback_reason")
            )
        optional_defaults = {
            "_lifecycle_episode_id": pl.lit(0, dtype=pl.Int32),
            "_lifecycle_evidence": pl.lit(
                "official_lifecycle:no_prior_terminal",
                dtype=pl.String,
            ),
            "_previous_official_session": pl.lit(None, dtype=pl.Date),
            "_previous_session_next_reference": pl.lit(
                None,
                dtype=pl.Float64,
            ),
            "_previous_session_next_reference_eligible": pl.lit(False),
            "_official_listing_boundary": pl.lit(False),
            "_official_listing_evidence": pl.lit(None, dtype=pl.String),
        }
        missing_optional = [
            expression.alias(column)
            for column, expression in optional_defaults.items()
            if column not in frame.columns
        ]
        if missing_optional:
            frame = frame.with_columns(missing_optional)
        fallback_rows = int(
            frame.select(
                (pl.col("quote_source") == YAHOO_FALLBACK_SOURCE_NAME)
                .sum()
                .alias("fallback_rows")
            ).item()
        )
        output_source = (
            MIXED_FALLBACK_SOURCE_NAME if fallback_rows else OFFICIAL_SOURCE_NAME
        )
        episode_ids = frame["_lifecycle_episode_id"].to_numpy()
        explicit_factor = frame["source_factor"].to_numpy()
        source_adjusted_factor = _source_adjustment_factors(
            frame["source_adjclose"].to_numpy(),
            frame["_legacy_source_id"].to_numpy(),
            episode_ids,
        )
        factor_override = np.where(
            np.isfinite(explicit_factor),
            explicit_factor,
            source_adjusted_factor,
        )
        fallback_factor = frame["_yahoo_fallback_factor"].to_numpy()
        official_factor_unavailable = (
            ~np.isfinite(factor_override)
            & ~np.isfinite(frame["reference_override"].to_numpy())
            & ~np.isfinite(frame["signed_change"].to_numpy())
            & np.isfinite(frame["Trading_Volume"].to_numpy())
            & (frame["Trading_Volume"].to_numpy() > 0.0)
        )
        previous_reference = frame[
            "_previous_session_next_reference"
        ].to_numpy()
        close_values = frame["close"].to_numpy()
        same_episode_as_previous = np.zeros(frame.height, dtype=bool)
        if frame.height > 1:
            same_episode_as_previous[1:] = episode_ids[1:] == episode_ids[:-1]
        uses_previous_session_next_reference = (
            official_factor_unavailable
            & frame[
                "_previous_session_next_reference_eligible"
            ].to_numpy()
            .astype(bool)
            & same_episode_as_previous
            & np.isfinite(previous_reference)
            & (previous_reference > 0.0)
            & np.isfinite(close_values)
            & (close_values > 0.0)
        )
        previous_reference_factor = np.divide(
            close_values,
            previous_reference,
            out=np.full(close_values.shape, np.nan, dtype=np.float64),
            where=(
                np.isfinite(previous_reference)
                & (previous_reference > 0.0)
                & np.isfinite(close_values)
            ),
        )
        factor_override = np.where(
            uses_previous_session_next_reference,
            previous_reference_factor,
            factor_override,
        )
        uses_fallback_adjustment = (
            official_factor_unavailable
            & ~uses_previous_session_next_reference
            & np.isfinite(fallback_factor)
            & (fallback_factor > 0.0)
        )
        factor_override = np.where(
            uses_fallback_adjustment,
            fallback_factor,
            factor_override,
        )
        fallback_adjustment_rows = int(np.count_nonzero(uses_fallback_adjustment))
        previous_session_next_reference_adjustment_rows = int(
            np.count_nonzero(uses_previous_session_next_reference)
        )
        normalized_zero_ohlc_rows = int(
            frame.select(pl.col("_official_zero_ohlc_normalized").sum()).item()
            or 0
        )
        if fallback_adjustment_rows:
            output_source = MIXED_FALLBACK_SOURCE_NAME
        adjusted, missing = _normalized_reference_index(
            frame["close"].to_numpy(),
            frame["signed_change"].to_numpy(),
            frame["Trading_Volume"].to_numpy(),
            frame["reference_override"].to_numpy(),
            factor_override,
            episode_ids,
        )
        adjustment_source = np.asarray(
            frame["quote_source"].to_list(), dtype=object
        )
        adjustment_source[
            uses_previous_session_next_reference
        ] = PREVIOUS_SESSION_NEXT_REFERENCE_SOURCE_NAME
        adjustment_source[uses_fallback_adjustment] = YAHOO_FALLBACK_SOURCE_NAME

        # The normalized index remains an auditable quote artifact.  Boundary
        # and unverified extreme transitions are isolated at the *label* row
        # instead of rewriting prices or silently relaxing the anomaly limit.
        listing_boundary = frame["_official_listing_boundary"].to_numpy().astype(
            bool, copy=False
        )
        episode_boundary = np.zeros(frame.height, dtype=bool)
        if frame.height > 1:
            episode_boundary[1:] = episode_ids[1:] != episode_ids[:-1]
        extreme_adjusted_transition = np.zeros(frame.height, dtype=bool)
        if frame.height > 1:
            valid_adjusted_pair = (
                np.isfinite(adjusted[1:])
                & np.isfinite(adjusted[:-1])
                & (adjusted[1:] > 0.0)
                & (adjusted[:-1] > 0.0)
            )
            adjusted_log_change = np.zeros(frame.height - 1, dtype=np.float64)
            adjusted_log_change[valid_adjusted_pair] = np.abs(
                np.log(
                    adjusted[1:][valid_adjusted_pair]
                    / adjusted[:-1][valid_adjusted_pair]
                )
            )
            extreme_adjusted_transition[1:] = (
                valid_adjusted_pair
                & (adjusted_log_change > math.log(3.0) + 1e-9)
            )
        quote_source_values = np.asarray(
            frame["quote_source"].to_list(), dtype=object
        )
        adjustment_source_values = np.asarray(adjustment_source, dtype=object)
        # The emitted lineage is the evidence available to downstream label
        # audits.  In particular, merely having a corporate-action reference
        # on the row cannot verify an extreme transition when a Yahoo factor
        # takes precedence in _normalized_reference_index (factor_override is
        # evaluated before reference_override).  Fail closed whenever either
        # the selected quote or selected adjustment remains Yahoo-backed.
        unverified_extreme = extreme_adjusted_transition & (
            (quote_source_values == YAHOO_FALLBACK_SOURCE_NAME)
            | (adjustment_source_values == YAHOO_FALLBACK_SOURCE_NAME)
        )
        return_quarantined = np.zeros(frame.height, dtype=bool)
        return_quarantine_reason = np.full(frame.height, None, dtype=object)
        for destination_index in range(1, frame.height):
            reason: str | None = None
            if episode_boundary[destination_index]:
                reason = "official_lifecycle_episode_boundary"
            elif listing_boundary[destination_index]:
                reason = "official_listing_boundary"
            elif unverified_extreme[destination_index]:
                reason = "unverified_extreme_adjusted_return"
            if reason is not None:
                source_index = destination_index - 1
                return_quarantined[source_index] = True
                return_quarantine_reason[source_index] = reason
        quarantine_counts = {
            reason: int(np.count_nonzero(return_quarantine_reason == reason))
            for reason in RETURN_QUARANTINE_REASONS
        }
        previous_session_dates = frame["_previous_official_session"].to_list()
        adjustment_reference_dates = [
            previous_session_dates[index]
            if uses_previous_session_next_reference[index]
            else None
            for index in range(frame.height)
        ]
        adjustment_reference_prices = np.where(
            uses_previous_session_next_reference,
            previous_reference,
            np.nan,
        )
        quotes = frame.select(
            "date",
            "open",
            "max",
            "min",
            "close",
            "Trading_Volume",
            pl.col("quote_source").alias("data_source"),
            "fallback_reason",
            pl.col("_lifecycle_episode_id").alias("lifecycle_episode_id"),
            pl.col("_lifecycle_evidence").alias("lifecycle_evidence"),
            pl.col("_official_listing_evidence").alias("official_listing_evidence"),
            "_official_zero_ohlc_normalized",
        ).with_columns(
            pl.Series("adjustment_source", adjustment_source, dtype=pl.String),
            pl.Series(
                "adjustment_reference_date",
                adjustment_reference_dates,
                dtype=pl.Date,
            ),
            pl.Series(
                "adjustment_reference_price",
                adjustment_reference_prices,
                dtype=pl.Float64,
            ).fill_nan(None),
            pl.when(
                pl.Series(
                    "_uses_previous_session_next_reference",
                    uses_previous_session_next_reference,
                    dtype=pl.Boolean,
                )
            )
            .then(pl.lit("previous_official_session_next_reference"))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("adjustment_reference_kind"),
            pl.when(pl.col("_official_zero_ohlc_normalized"))
            .then(pl.lit("official_close_flat_bar"))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("ohlc_normalization"),
            pl.Series("adjclose", adjusted),
            pl.Series("return_quarantined", return_quarantined, dtype=pl.Boolean),
            pl.Series(
                "return_quarantine_reason",
                return_quarantine_reason.tolist(),
                dtype=pl.String,
            ),
        ).drop("_official_zero_ohlc_normalized")
        first = frame["date"].min()
        last = frame["date"].max()
        market = str(frame["market"][-1])
        name = str(frame["name"][-1] or symbol)
        if not dry_run:
            _write_official_quote_parquet(
                quotes,
                path,
                checked_through=requested_end_date,
                source=output_source,
            )
        return BuildResult(
            symbol=symbol,
            security_type=str(frame["security_type"][-1]),
            market=market,
            name=name,
            status="planned" if dry_run else "created",
            rows=frame.height,
            adjusted_rows=int(np.count_nonzero(np.isfinite(adjusted))),
            missing_adjustment_rows=missing,
            first_date=str(first),
            last_date=str(last),
            output_path=str(path),
            source=output_source,
            fallback_rows=fallback_rows,
            fallback_adjustment_rows=fallback_adjustment_rows,
            previous_session_next_reference_adjustment_rows=(
                previous_session_next_reference_adjustment_rows
            ),
            normalized_zero_ohlc_rows=normalized_zero_ohlc_rows,
            return_quarantined_rows=int(np.count_nonzero(return_quarantined)),
            lifecycle_episode_quarantined_rows=quarantine_counts[
                "official_lifecycle_episode_boundary"
            ],
            listing_boundary_quarantined_rows=quarantine_counts[
                "official_listing_boundary"
            ],
            unverified_extreme_quarantined_rows=quarantine_counts[
                "unverified_extreme_adjusted_return"
            ],
        )
    except Exception as exc:
        return BuildResult(
            symbol=symbol,
            security_type="",
            market="",
            name=symbol,
            status="failed",
            rows=0,
            adjusted_rows=0,
            missing_adjustment_rows=0,
            first_date=None,
            last_date=None,
            output_path=str(path),
            message=f"{type(exc).__name__}: {exc}",
        )


def main() -> None:
    args = parse_args()
    end_date_text = getattr(args, "end_date", None)
    end_date = date.fromisoformat(end_date_text) if end_date_text else None
    if args.legacy_official_ohlcv and not args.legacy_source_name:
        raise ValueError("--legacy-source-name is required with --legacy-official-ohlcv")
    if args.legacy_source_name and not args.legacy_official_ohlcv:
        raise ValueError("--legacy-official-ohlcv is required with --legacy-source-name")
    if args.fallback_ohlcv and not args.fallback_source_name:
        raise ValueError("--fallback-source-name is required with --fallback-ohlcv")
    if args.fallback_source_name and not args.fallback_ohlcv:
        raise ValueError("--fallback-ohlcv is required with --fallback-source-name")
    if args.fallback_source_name and args.fallback_source_name != YAHOO_FALLBACK_SOURCE_NAME:
        raise ValueError(
            f"unsupported fallback source {args.fallback_source_name!r}; "
            f"expected {YAHOO_FALLBACK_SOURCE_NAME!r}"
        )
    for fallback_path in args.fallback_ohlcv:
        _validate_yahoo_fallback_archive(fallback_path)
    _validate_download_receipts(
        args.input_dir,
        allow_incomplete=bool(args.allow_incomplete_source),
        allow_daily_publication_lag=bool(
            getattr(args, "allow_daily_publication_lag", False)
        ),
    )
    frame, receipts, legacy_receipts, fallback_receipts, merge_stats = _official_frame(
        args.input_dir,
        list(args.legacy_official_ohlcv),
        list(args.fallback_ohlcv),
        end_date=end_date,
    )
    if frame.is_empty():
        raise RuntimeError(f"no official rows remain through end_date={end_date_text!r}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    requested_end_date = str(end_date or frame["date"].max())
    groups = frame.partition_by("symbol", as_dict=True, maintain_order=False)
    results: list[BuildResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _write_symbol,
                args.output_dir,
                str(key[0] if isinstance(key, tuple) else key),
                symbol_frame,
                requested_end_date=requested_end_date,
                dry_run=bool(args.dry_run),
            ): key
            for key, symbol_frame in groups.items()
        }
        for future in as_completed(futures):
            results.append(future.result())
    failed = [result for result in results if result.status == "failed"]
    if failed:
        raise RuntimeError(f"official symbol parquet writes failed: {[asdict(item) for item in failed[:10]]}")
    previous_session_next_reference_adjustment_rows = sum(
        result.previous_session_next_reference_adjustment_rows
        for result in results
    )
    if (
        previous_session_next_reference_adjustment_rows
        != merge_stats.previous_session_next_reference_candidate_rows
    ):
        raise RuntimeError(
            "previous-session next-reference reconciliation failed: "
            f"written={previous_session_next_reference_adjustment_rows} "
            "candidates="
            f"{merge_stats.previous_session_next_reference_candidate_rows}"
        )

    target_symbols = {result.symbol for result in results}
    stale_symbols_removed = _remove_stale_official_symbol_files(
        args.output_dir,
        target_symbols,
        dry_run=bool(args.dry_run),
    )
    manifest = pl.DataFrame(
        {
            "code": [result.symbol for result in results],
            "name": [result.name for result in results],
            "market": [result.market for result in results],
            "security_type": [result.security_type for result in results],
            "source": [result.source for result in results],
        }
    ).sort("code")
    if not args.dry_run:
        temporary = args.output_dir / "symbols.csv.tmp"
        manifest.write_csv(temporary)
        temporary.replace(args.output_dir / "symbols.csv")
    _update_return_price_provenance(
        args.output_dir,
        target_symbols,
        fallback_symbols={
            result.symbol
            for result in results
            if result.fallback_rows > 0 or result.fallback_adjustment_rows > 0
        },
        dry_run=bool(args.dry_run),
    )
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    summary = {
        "schema_version": OFFICIAL_SYMBOL_BUILD_SCHEMA_VERSION,
        "source": (
            MIXED_FALLBACK_SOURCE_NAME
            if any(
                result.fallback_rows > 0 or result.fallback_adjustment_rows > 0
                for result in results
            )
            else OFFICIAL_SOURCE_NAME
        ),
        "adjusted_price_method": OFFICIAL_SYMBOL_ADJUSTED_PRICE_METHOD,
        "source_receipts": receipts,
        "legacy_source_name": str(args.legacy_source_name or ""),
        "legacy_source_receipts": legacy_receipts,
        "fallback_source_name": str(args.fallback_source_name or ""),
        "fallback_source_receipts": fallback_receipts,
        "session_calendar_rows": merge_stats.session_calendar_rows,
        "session_calendar_receipt": merge_stats.session_calendar_receipt,
        "session_calendar_summary_receipt": (
            merge_stats.session_calendar_summary_receipt
        ),
        "dropped_off_calendar_fallback_rows": (
            merge_stats.dropped_off_calendar_fallback_rows
        ),
        "dropped_off_calendar_fallback_examples": (
            merge_stats.dropped_off_calendar_fallback_examples
        ),
        "lifecycle_source_receipts": merge_stats.lifecycle_source_receipts,
        "lifecycle_terminal_events": merge_stats.lifecycle_terminal_events,
        "lifecycle_nonterminal_transfer_events": (
            merge_stats.lifecycle_nonterminal_transfer_events
        ),
        "lifecycle_nonterminal_transfer_examples": (
            merge_stats.lifecycle_nonterminal_transfer_examples
        ),
        "fallback_rows_before_lifecycle_filter": (
            merge_stats.fallback_rows_before_lifecycle_filter
        ),
        "fallback_rows_after_lifecycle_filter": (
            merge_stats.fallback_rows_after_lifecycle_filter
        ),
        "dropped_post_terminal_fallback_rows": (
            merge_stats.dropped_post_terminal_fallback_rows
        ),
        "dropped_post_terminal_fallback_examples": (
            merge_stats.dropped_post_terminal_fallback_examples
        ),
        "rows": int(frame.height),
        "symbols": len(results),
        "security_type_counts": {
            security_type: sum(result.security_type == security_type for result in results)
            for security_type in ("stock", "etf")
        },
        "stale_symbols_removed": stale_symbols_removed,
        "first_date": str(frame["date"].min()),
        "last_date": str(frame["date"].max()),
        "adjusted_rows": sum(result.adjusted_rows for result in results),
        "missing_adjustment_rows": sum(result.missing_adjustment_rows for result in results),
        "fallback_rows": sum(result.fallback_rows for result in results),
        "fallback_adjustment_rows": sum(
            result.fallback_adjustment_rows for result in results
        ),
        "previous_session_next_reference_candidate_rows": (
            merge_stats.previous_session_next_reference_candidate_rows
        ),
        "previous_session_next_reference_candidate_examples": (
            merge_stats.previous_session_next_reference_candidate_examples or []
        ),
        "previous_session_next_reference_adjustment_rows": (
            previous_session_next_reference_adjustment_rows
        ),
        "normalized_zero_ohlc_rows": sum(
            result.normalized_zero_ohlc_rows for result in results
        ),
        "return_quarantined_rows": sum(
            result.return_quarantined_rows for result in results
        ),
        "lifecycle_episode_quarantined_rows": sum(
            result.lifecycle_episode_quarantined_rows for result in results
        ),
        "listing_boundary_quarantined_rows": sum(
            result.listing_boundary_quarantined_rows for result in results
        ),
        "unverified_extreme_quarantined_rows": sum(
            result.unverified_extreme_quarantined_rows for result in results
        ),
        "official_unusable_ohlcv_rows": merge_stats.official_unusable_ohlcv_rows,
        "fallback_replaced_unusable_official_rows": (
            merge_stats.fallback_replaced_unusable_official_rows
        ),
        "unfilled_unusable_official_rows": (
            merge_stats.unfilled_unusable_official_rows
        ),
        "unfilled_unusable_official_examples": (
            merge_stats.unfilled_unusable_official_examples
        ),
        "fallback_symbols": sum(
            result.fallback_rows > 0 or result.fallback_adjustment_rows > 0
            for result in results
        ),
        "status_counts": status_counts,
        "dry_run": bool(args.dry_run),
    }
    summary_path = args.summary_path or args.output_dir / "official_symbol_build_summary.json"
    report_path = args.report_path or args.output_dir / "official_symbol_build_report.csv"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pl.DataFrame([asdict(result) for result in sorted(results, key=lambda item: item.symbol)]).write_csv(
        report_path
    )
    print(
        f"[tw-official-symbols] rows={frame.height} symbols={len(results)} "
        f"missing_adjustments={summary['missing_adjustment_rows']} summary={summary_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
