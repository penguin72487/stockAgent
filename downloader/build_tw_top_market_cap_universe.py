from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import polars as pl

try:
    from downloader.common import (
        SharedRateLimiter,
        describe_rate_limit,
        resolve_request_interval,
        retry_delay_seconds,
    )
except ModuleNotFoundError:  # direct script execution
    from common import (
        SharedRateLimiter,
        describe_rate_limit,
        resolve_request_interval,
        retry_delay_seconds,
    )


TWSE_COMPANIES_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_CLOSE_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_COMPANIES_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_MARKET_VALUE_URL = "https://www.tpex.org.tw/openapi/v1/tpex_daily_market_value"
SOURCE_NAME = "twse_tpex_official_market_cap"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a reproducible Taiwan listed/OTC top-market-cap stock universe "
            "from official TWSE and TPEx endpoints. ETFs and other non-company "
            "securities are excluded by joining company master data."
        )
    )
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_microstructure/universe"),
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--retry-base", type=float, default=0.6)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help=(
            "Host-global minimum interval for TW official endpoints. The "
            "default is the documented stockAgent safety policy, not an "
            "upstream-published numeric quota."
        ),
    )
    return parser.parse_args()


def _fetch_json(
    url: str,
    *,
    timeout: float,
    limiter: SharedRateLimiter,
    max_retries: int,
    retry_base: float,
) -> tuple[list[dict[str, Any]], bytes]:
    last_error: Exception | None = None
    for attempt in range(max(0, int(max_retries)) + 1):
        limiter.wait()
        request = Request(url, headers={"User-Agent": "stockAgent/1.0"})
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                body = response.read()
            payload = json.loads(body)
            if not isinstance(payload, list) or not all(
                isinstance(row, dict) for row in payload
            ):
                raise RuntimeError(
                    f"official endpoint returned a non-tabular payload: {url}"
                )
            return payload, body
        except HTTPError as exc:
            last_error = exc
            if (
                exc.code not in {408, 425, 429, 500, 502, 503, 504}
                or attempt >= max_retries
            ):
                raise
            limiter.defer(
                retry_delay_seconds(
                    attempt,
                    base=retry_base,
                    retry_after=(
                        exc.headers.get("Retry-After")
                        if exc.headers is not None
                        else None
                    ),
                )
            )
        except (URLError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= max_retries:
                raise
            limiter.defer(retry_delay_seconds(attempt, base=retry_base))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"official endpoint failed without explicit error: {url}")


def _number(value: Any) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if not text or text in {"--", "---", "N/A"}:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    return result if result > 0.0 else None


def _roc_date(value: Any) -> date:
    text = str(value or "").strip()
    if len(text) != 7 or not text.isdigit():
        raise ValueError(f"invalid ROC date: {value!r}")
    return date(int(text[:3]) + 1911, int(text[3:5]), int(text[5:7]))


def build_universe(
    twse_companies: list[dict[str, Any]],
    twse_close: list[dict[str, Any]],
    tpex_companies: list[dict[str, Any]],
    tpex_market_value: list[dict[str, Any]],
    *,
    count: int,
) -> pl.DataFrame:
    if count <= 0:
        raise ValueError("count must be positive")
    twse_company_map = {
        str(row.get("公司代號") or "").strip(): row
        for row in twse_companies
        if str(row.get("公司代號") or "").strip()
    }
    tpex_company_map = {
        str(row.get("SecuritiesCompanyCode") or "").strip(): row
        for row in tpex_companies
        if str(row.get("SecuritiesCompanyCode") or "").strip()
    }
    rows: list[dict[str, Any]] = []
    for quote in twse_close:
        code = str(quote.get("Code") or "").strip()
        company = twse_company_map.get(code)
        close = _number(quote.get("ClosingPrice"))
        shares = _number(
            company.get("已發行普通股數或TDR原股發行股數") if company else None
        )
        if company is None or close is None or shares is None:
            continue
        source_date = _roc_date(quote.get("Date"))
        rows.append(
            {
                "symbol": code,
                "name": str(
                    company.get("公司簡稱") or quote.get("Name") or code
                ).strip(),
                "market": "twse",
                "market_cap_ntd": close * shares,
                "close": close,
                "issued_common_shares": int(shares),
                "source_date": source_date,
                "market_cap_method": "issued_common_shares_x_close",
            }
        )
    for item in tpex_market_value:
        code = str(item.get("SecuritiesCompanyCode") or "").strip()
        company = tpex_company_map.get(code)
        market_value_million = _number(item.get("MarketValue"))
        close = _number(item.get("ClosePrice"))
        shares = _number(item.get("Capitals"))
        if (
            company is None
            or market_value_million is None
            or close is None
            or shares is None
        ):
            continue
        source_date = _roc_date(item.get("Date"))
        rows.append(
            {
                "symbol": code,
                "name": str(
                    company.get("CompanyAbbreviation")
                    or item.get("CompanyName")
                    or code
                ).strip(),
                "market": "tpex",
                "market_cap_ntd": market_value_million * 1_000_000.0,
                "close": close,
                "issued_common_shares": int(shares),
                "source_date": source_date,
                "market_cap_method": "tpex_official_market_value",
            }
        )
    if len(rows) < count:
        raise RuntimeError(f"only {len(rows)} valid company market caps; need {count}")
    frame = (
        pl.DataFrame(rows)
        .sort(["market_cap_ntd", "symbol"], descending=[True, False])
        .head(count)
        .with_row_index("market_cap_rank", offset=1)
        .with_columns(
            pl.col("market_cap_rank").cast(pl.Int32),
            pl.col("issued_common_shares").cast(pl.Int64),
            pl.col("market_cap_ntd").cast(pl.Float64),
        )
    )
    if frame["symbol"].n_unique() != frame.height:
        raise RuntimeError(
            "combined official market-cap universe contains duplicate symbols"
        )
    return frame


def _atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(body)
    os.replace(temporary, path)


def main() -> None:
    args = parse_args()
    request_interval = resolve_request_interval("tw_public", args.request_interval)
    limiter = SharedRateLimiter(request_interval, name="tw_public")
    print(
        f"[tw-market-cap] {describe_rate_limit('tw_public', request_interval)}",
        flush=True,
    )
    endpoints = {
        "twse_companies": TWSE_COMPANIES_URL,
        "twse_close": TWSE_CLOSE_URL,
        "tpex_companies": TPEX_COMPANIES_URL,
        "tpex_market_value": TPEX_MARKET_VALUE_URL,
    }
    payloads: dict[str, list[dict[str, Any]]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    raw_dir = args.output_dir / "raw"
    for name, url in endpoints.items():
        payload, body = _fetch_json(
            url,
            timeout=float(args.timeout),
            limiter=limiter,
            max_retries=max(0, int(args.max_retries)),
            retry_base=max(0.1, float(args.retry_base)),
        )
        raw_path = raw_dir / f"{name}.json"
        _atomic_write(raw_path, body)
        payloads[name] = payload
        receipts[name] = {
            "url": url,
            "path": str(raw_path),
            "rows": len(payload),
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        }
    frame = build_universe(
        payloads["twse_companies"],
        payloads["twse_close"],
        payloads["tpex_companies"],
        payloads["tpex_market_value"],
        count=int(args.count),
    )
    csv_path = args.output_dir / f"top_{int(args.count)}.csv"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    temporary_csv = csv_path.with_suffix(".csv.tmp")
    frame.write_csv(temporary_csv)
    os.replace(temporary_csv, csv_path)
    summary = {
        "schema_version": 1,
        "source": SOURCE_NAME,
        "count": frame.height,
        "latest_source_date": str(frame["source_date"].max()),
        "oldest_source_date": str(frame["source_date"].min()),
        "twse_symbols": frame.filter(pl.col("market") == "twse").height,
        "tpex_symbols": frame.filter(pl.col("market") == "tpex").height,
        "universe_path": str(csv_path),
        "universe_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "official_receipts": receipts,
        "written_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _atomic_write(
        args.output_dir / f"top_{int(args.count)}.summary.json",
        (
            json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        ).encode(),
    )
    print(
        f"[tw-market-cap] count={frame.height} twse={summary['twse_symbols']} "
        f"tpex={summary['tpex_symbols']} source_date={summary['latest_source_date']} "
        f"output={csv_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
