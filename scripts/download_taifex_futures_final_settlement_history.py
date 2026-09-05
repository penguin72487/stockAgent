#!/usr/bin/env python3
"""Build receipt-backed official final settlements for all supported futures.

The daily TAIFEX portfolio file contains a *daily* settlement/close valuation.
That value is not interchangeable with the exchange's final settlement price.
This collector therefore preserves the official index-futures and single-stock/
ETF-futures HTML receipts, normalizes them to one strict key, and writes an
atomic manifest suitable for carry-to-expiry training.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import io
from pathlib import Path
import re
import sys
from typing import Final, Iterable
from urllib import parse

import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.taifex_daily_download_common import (  # noqa: E402
    atomic_write_json,
    parse_iso_date,
    sha256_path,
)
from downloader.artifact_io import (  # noqa: E402
    atomic_write_bytes,
    atomic_write_parquet,
)
from downloader.common import (  # noqa: E402
    SharedRateLimiter,
    resolve_request_interval,
)
from downloader.http_transport import (  # noqa: E402
    HttpRequestPolicy,
    ResilientHttpTransport,
)
from stockagent.data.tw_stock_context_futures_portfolio import (  # noqa: E402
    TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION,
)


INDEX_FINAL_SETTLEMENT_PAGE: Final[str] = "https://www.taifex.com.tw/cht/5/futIndxFSP"
STOCK_FINAL_SETTLEMENT_PAGE: Final[str] = "https://www.taifex.com.tw/cht/5/sSFFSP"
INDEX_COMMODITY_IDS: Final[tuple[str, ...]] = (
    "1",
    "40",
    "3",
    "4",
    "5",
    "12",
    "13",
    "15",
    "32",
    "34",
    "35",
    "37",
    "38",
    "21",
    "24",
    "27",
    "28",
    "33",
    "39",
    "36",
)
_PRODUCT_CODE = re.compile(r"\b[A-Z][A-Z0-9]{1,3}\b")


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "settlement_date": pl.Date,
            "product": pl.String,
            "contract": pl.String,
            "final_settlement_price": pl.Float64,
            "source_kind": pl.String,
            "source_file": pl.String,
            "source_sha256": pl.String,
            "source_url": pl.String,
        }
    )


def _normalized_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()


def _positive_float(value: object) -> float | None:
    text = str(value).strip().replace(",", "")
    try:
        result = float(text)
    except (TypeError, ValueError):
        return None
    return result if result > 0.0 else None


def _parsed_date(value: object) -> date | None:
    parsed = pd.to_datetime(value, format="%Y/%m/%d", errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _records_frame(records: list[dict[str, object]]) -> pl.DataFrame:
    if not records:
        return _empty_frame()
    return pl.from_dicts(records, schema=_empty_frame().schema, strict=False).sort(
        "settlement_date", "product", "contract"
    )


def parse_index_futures_final_settlement_html(
    body: bytes,
    *,
    start_date: date,
    end_date: date,
    source_file: str,
    source_sha256: str,
    source_url: str,
) -> pl.DataFrame:
    """Normalize the official wide index-futures final-settlement table."""

    text = body.decode("utf-8")
    tables = pd.read_html(io.StringIO(text))
    candidates = [table for table in tables if table.shape[1] >= 3]
    if len(candidates) != 1:
        raise ValueError(
            "expected exactly one index-futures final-settlement table, "
            f"found {len(candidates)}"
        )
    table = candidates[0]
    records: list[dict[str, object]] = []
    for _, row in table.iterrows():
        settlement_date = _parsed_date(row.iloc[0])
        contract = _normalized_text(row.iloc[1]).upper()
        if (
            settlement_date is None
            or not (start_date <= settlement_date <= end_date)
            or not re.fullmatch(r"[0-9]{6}(?:W[1-5])?", contract)
        ):
            continue
        for column_index, column in enumerate(table.columns[2:], start=2):
            price = _positive_float(row.iloc[column_index])
            if price is None:
                continue
            products = _PRODUCT_CODE.findall(_normalized_text(column).upper())
            if not products:
                raise ValueError(
                    f"index final-settlement column has no product code: {column!r}"
                )
            for product in products:
                records.append(
                    {
                        "settlement_date": settlement_date,
                        "product": product,
                        "contract": contract,
                        "final_settlement_price": price,
                        "source_kind": "official_index_futures_html",
                        "source_file": source_file,
                        "source_sha256": source_sha256,
                        "source_url": source_url,
                    }
                )
    return _records_frame(records)


def parse_stock_futures_final_settlement_html(
    body: bytes,
    *,
    start_date: date,
    end_date: date,
    source_file: str,
    source_sha256: str,
    source_url: str,
) -> pl.DataFrame:
    """Normalize the official single-stock/ETF-futures settlement table."""

    text = body.decode("utf-8")
    tables = pd.read_html(io.StringIO(text))
    candidates = [table for table in tables if table.shape[1] >= 6]
    if len(candidates) != 1:
        raise ValueError(
            "expected exactly one stock-futures final-settlement table, "
            f"found {len(candidates)}"
        )
    table = candidates[0]
    records: list[dict[str, object]] = []
    for _, row in table.iterrows():
        product = _normalized_text(row.iloc[1]).upper()
        settlement_date = _parsed_date(row.iloc[3])
        contract = _normalized_text(row.iloc[4]).upper()
        price = _positive_float(row.iloc[5])
        if (
            not re.fullmatch(r"[A-Z][A-Z0-9]{1,3}", product)
            or settlement_date is None
            or not (start_date <= settlement_date <= end_date)
            or not re.fullmatch(r"[0-9]{6}", contract)
            or price is None
        ):
            continue
        records.append(
            {
                "settlement_date": settlement_date,
                "product": product,
                "contract": contract,
                "final_settlement_price": price,
                "source_kind": "official_stock_etf_futures_html",
                "source_file": source_file,
                "source_sha256": source_sha256,
                "source_url": source_url,
            }
        )
    return _records_frame(records)


def _download_html(
    url: str,
    target: Path,
    *,
    refresh: bool,
    attempts: int = 3,
    transport: ResilientHttpTransport | None = None,
) -> Path:
    if not refresh and target.is_file() and target.stat().st_size > 1_000:
        return target
    client = transport or ResilientHttpTransport(
        HttpRequestPolicy(
            provider="taifex_public",
            timeout_seconds=120,
            max_retries=max(0, attempts - 1),
            retry_base_seconds=0.5,
        )
    )
    response = client.request_bytes(
        url,
        headers={"User-Agent": "stockAgent/taifex-futures-final-settlement-research"},
    )
    body = response.body
    content_type = str(
        response.headers.get("Content-Type")
        or response.headers.get("content-type")
        or ""
    )
    if len(body) < 1_000 or "html" not in content_type.casefold():
        raise RuntimeError(f"TAIFEX response is not non-empty HTML: {url}")
    atomic_write_bytes(target, body, durable=True)
    return target


def _months(start_date: date, end_date: date) -> Iterable[tuple[int, int]]:
    year, month = start_date.year, start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        yield year, month
        month += 1
        if month == 13:
            year += 1
            month = 1


def _index_url(year: int) -> str:
    query: list[tuple[str, str]] = [
        *(("commodityIds", value) for value in INDEX_COMMODITY_IDS),
        ("start_year", str(year)),
        ("start_month", "01"),
        ("end_year", str(year)),
        ("end_month", "12"),
    ]
    return f"{INDEX_FINAL_SETTLEMENT_PAGE}?{parse.urlencode(query)}"


def _stock_url(year: int, month: int) -> str:
    return f"{STOCK_FINAL_SETTLEMENT_PAGE}?" + parse.urlencode(
        {
            "down_type": "1",
            "queryYear": f"{year:04d}",
            "queryMonth": f"{month:02d}",
        }
    )


def _atomic_write_parquet(frame: pl.DataFrame, target: Path) -> None:
    atomic_write_parquet(
        target,
        frame,
        compression="zstd",
        write_statistics=True,
        durable=True,
    )


def _parse_date_arg(value: str) -> date:
    try:
        return parse_iso_date(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected ISO date YYYY-MM-DD, got {value!r}"
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=_parse_date_arg, default=date(2014, 1, 1))
    parser.add_argument(
        "--end-date", type=_parse_date_arg, default=date.today() - timedelta(days=1)
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_tw_futures/final_settlement_v1")
    )
    parser.add_argument(
        "--portfolio-path",
        type=Path,
        default=Path(
            "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet"
        ),
        help=(
            "also query every stock/ETF contract delivery month observed in this "
            "portfolio window, including far contracts settled early"
        ),
    )
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--request-delay-seconds", type=float, default=0.05)
    args = parser.parse_args()
    if args.end_date < args.start_date:
        parser.error("--end-date precedes --start-date")
    if args.request_delay_seconds < 0.0:
        parser.error("--request-delay-seconds cannot be negative")

    request_interval = resolve_request_interval(
        "taifex_public", args.request_delay_seconds
    )
    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="taifex_public",
            timeout_seconds=120,
            max_retries=2,
            retry_base_seconds=0.5,
        ),
        limiter=SharedRateLimiter(request_interval, name="taifex_public"),
    )

    output_dir = args.output_dir.expanduser().resolve()
    raw_dir = output_dir / "raw"
    frames: list[pl.DataFrame] = []
    receipts: list[dict[str, object]] = []

    for year in range(args.start_date.year, args.end_date.year + 1):
        period_start = max(args.start_date, date(year, 1, 1))
        period_end = min(args.end_date, date(year, 12, 31))
        url = _index_url(year)
        receipt = _download_html(
            url,
            raw_dir / "index" / f"{year:04d}.html",
            refresh=bool(args.refresh),
            transport=transport,
        )
        digest = sha256_path(receipt)
        parsed_frame = parse_index_futures_final_settlement_html(
            receipt.read_bytes(),
            start_date=period_start,
            end_date=period_end,
            source_file=str(receipt),
            source_sha256=digest,
            source_url=url,
        )
        frames.append(parsed_frame)
        receipts.append(
            {
                "kind": "index_futures",
                "period": f"{year:04d}",
                "path": str(receipt),
                "bytes": receipt.stat().st_size,
                "sha256": digest,
                "rows": parsed_frame.height,
                "url": url,
            }
        )

    query_months = set(_months(args.start_date, args.end_date))
    portfolio_path = args.portfolio_path.expanduser().resolve()
    portfolio_sha256: str | None = None
    if portfolio_path.is_file():
        portfolio_sha256 = sha256_path(portfolio_path)
        portfolio_contract_months = (
            pl.scan_parquet(portfolio_path)
            .filter(
                pl.col("date").is_between(args.start_date, args.end_date)
                & pl.col("asset_class").is_in(["stock_future", "etf_future"])
            )
            .select(
                pl.col("contract")
                .cast(pl.String)
                .str.extract(r"^(\d{4})(\d{2})", 0)
                .alias("contract_month")
            )
            .drop_nulls()
            .unique()
            .collect()["contract_month"]
            .to_list()
        )
        for value in portfolio_contract_months:
            matched = re.fullmatch(r"(\d{4})(\d{2})", str(value))
            if matched is not None:
                query_months.add((int(matched.group(1)), int(matched.group(2))))

    for year, month in sorted(query_months):
        url = _stock_url(year, month)
        receipt = _download_html(
            url,
            raw_dir / "stock_etf" / f"{year:04d}-{month:02d}.html",
            refresh=bool(args.refresh),
            transport=transport,
        )
        digest = sha256_path(receipt)
        parsed_frame = parse_stock_futures_final_settlement_html(
            receipt.read_bytes(),
            # The SSF page is selected by *delivery month*, but contract
            # adjustments can create an official final settlement months
            # earlier. Filter against the requested research window, not the
            # queried delivery month.
            start_date=args.start_date,
            end_date=args.end_date,
            source_file=str(receipt),
            source_sha256=digest,
            source_url=url,
        )
        frames.append(parsed_frame)
        receipts.append(
            {
                "kind": "stock_etf_futures",
                "period": f"{year:04d}-{month:02d}",
                "path": str(receipt),
                "bytes": receipt.stat().st_size,
                "sha256": digest,
                "rows": parsed_frame.height,
                "url": url,
            }
        )

    combined = pl.concat(frames, how="vertical") if frames else _empty_frame()
    if combined.height == 0:
        raise RuntimeError("official TAIFEX pages produced no final settlements")
    conflict = (
        combined.group_by("settlement_date", "product", "contract")
        .agg(
            pl.len().alias("rows"),
            pl.col("final_settlement_price").n_unique().alias("prices"),
        )
        .filter(pl.col("prices") != 1)
    )
    if conflict.height:
        raise RuntimeError("conflicting official final settlement values")
    combined = combined.unique(
        subset=["settlement_date", "product", "contract"], keep="first"
    ).sort("settlement_date", "product", "contract")

    normalized_path = output_dir / "futures_final_settlement_history.parquet"
    _atomic_write_parquet(combined, normalized_path)
    output_sha = sha256_path(normalized_path)
    manifest = {
        "schema_version": TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_start_date": args.start_date.isoformat(),
        "requested_end_date": args.end_date.isoformat(),
        "official_sources": {
            "index_futures": INDEX_FINAL_SETTLEMENT_PAGE,
            "stock_etf_futures": STOCK_FINAL_SETTLEMENT_PAGE,
        },
        "key": ["settlement_date", "product", "contract"],
        "rows": combined.height,
        "first_settlement_date": combined["settlement_date"].min().isoformat(),
        "last_settlement_date": combined["settlement_date"].max().isoformat(),
        "product_count": combined["product"].n_unique(),
        "portfolio_contract_month_source": {
            "path": str(portfolio_path),
            "sha256": portfolio_sha256,
            "exists": portfolio_path.is_file(),
        },
        "receipts": receipts,
        "outputs": {
            "futures_final_settlement_history": {
                "path": str(normalized_path),
                "bytes": normalized_path.stat().st_size,
                "sha256": output_sha,
                "rows": combined.height,
            }
        },
    }
    atomic_write_json(output_dir / "manifest.json", manifest)
    print(
        f"built {normalized_path}: {combined.height:,} official settlements, "
        f"{manifest['first_settlement_date']}..{manifest['last_settlement_date']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
