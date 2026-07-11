from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import _http_get


TWSE_URL = "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
TPEX_CURRENT_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ"
TPEX_HISTORICAL_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQHis"


@dataclass(slots=True)
class FetchResult:
    market: str
    year: int
    url: str
    rows: list[dict[str, Any]]
    raw_path: str | None
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official TWSE/TPEx ex-right/ex-dividend reference prices by year."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--mode",
        choices=("rebuild", "repair", "daily"),
        default="repair",
        help="rebuild replaces all history; repair merges the requested years; daily refreshes recent years.",
    )
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--daily-overlap-years", type=int, default=2)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--skip-raw", action="store_true")
    return parser.parse_args()


def _parse_roc_date(value: Any) -> date | None:
    text = str(value or "").strip().replace("年", "/").replace("月", "/").replace("日", "")
    text = text.replace("-", "/")
    parts = [part for part in text.split("/") if part]
    if len(parts) != 3:
        return None
    try:
        year, month, day = (int(part) for part in parts)
        if year < 1911:
            year += 1911
        return date(year, month, day)
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "--", "---", "N/A", "nan", "null"}:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    return result if result == result and result not in {float("inf"), float("-inf")} else None


def _payload_rows(payload: Any, *, market: str, url: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("response is not a JSON object")
    stat = str(payload.get("stat", "OK")).strip().lower()
    if stat not in {"", "ok"}:
        raise ValueError(f"official response status is not OK: {payload.get('stat')!r}")
    if isinstance(payload.get("fields"), list):
        fields = payload["fields"]
        data = payload.get("data", [])
    else:
        tables = payload.get("tables")
        if not isinstance(tables, list) or not tables:
            return []
        fields = tables[0].get("fields", [])
        data = tables[0].get("data", [])
    if not isinstance(fields, list) or not isinstance(data, list):
        raise ValueError("official response has invalid fields/data arrays")
    output: list[dict[str, Any]] = []
    for values in data:
        if not isinstance(values, list):
            continue
        row = {str(field): values[idx] if idx < len(values) else None for idx, field in enumerate(fields)}
        event_date = _parse_roc_date(
            row.get("資料日期")
            or row.get("除權息日期")
            or row.get("除權除息交易日")
        )
        symbol = str(row.get("股票代號") or row.get("代號") or "").strip().upper()
        if event_date is None or re.fullmatch(r"[0-9A-Z]{4,6}", symbol) is None:
            continue
        reference = _number(
            row.get("除權息參考價")
            or row.get("除權參考價")
        )
        opening_reference = _number(
            row.get("開盤競價基準")
            or row.get("開始交易基準價")
            or row.get("開市交易基準價")
        )
        previous_close = _number(row.get("除權息前收盤價") or row.get("除權息前收盤價格"))
        reference = reference if reference is not None and reference > 0 else None
        opening_reference = (
            opening_reference
            if opening_reference is not None and opening_reference > 0
            else None
        )
        if reference is None and opening_reference is None:
            continue
        output.append(
            {
                "date": event_date,
                "symbol": symbol,
                "market": market,
                "reference_price": reference,
                "opening_reference_price": opening_reference,
                "previous_close": previous_close,
                "event_type": str(row.get("權/息") or row.get("除權息") or "").strip(),
                "source_url": url,
            }
        )
    return output


def _year_request(market: str, year: int, end_date: date) -> tuple[str, dict[str, str]]:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), end_date)
    if market == "twse":
        start = max(start, date(2003, 5, 5))
        return TWSE_URL, {
            "response": "json",
            "startDate": start.strftime("%Y%m%d"),
            "endDate": end.strftime("%Y%m%d"),
        }
    if year <= 2007:
        start = max(start, date(2000, 9, 1))
        return TPEX_HISTORICAL_URL, {
            "response": "json",
            "startDate": start.strftime("%Y/%m/%d"),
            "endDate": end.strftime("%Y/%m/%d"),
            "code": "",
        }
    return TPEX_CURRENT_URL, {
        "response": "json",
        "startDate": start.strftime("%Y/%m/%d"),
        "endDate": end.strftime("%Y/%m/%d"),
    }


def _fetch_year(
    market: str,
    year: int,
    end_date: date,
    args: argparse.Namespace,
) -> FetchResult:
    url, params = _year_request(market, year, end_date)
    try:
        response = _http_get(
            url,
            params=params,
            timeout=int(args.timeout),
            verify_ssl=True,
            retries=int(args.retries),
            retry_backoff=1.0,
        )
        payload = response.json()
        rows = _payload_rows(payload, market=market, url=response.url)
        raw_path: Path | None = None
        if not args.skip_raw:
            raw_dir = args.output_dir / "raw" / "tw_corporate_action_reference"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_path = raw_dir / f"{market}_{year}.json"
            temporary = raw_path.with_suffix(".json.tmp")
            temporary.write_bytes(response.content)
            os.replace(temporary, raw_path)
        return FetchResult(market, year, response.url, rows, str(raw_path) if raw_path else None)
    except Exception as exc:
        return FetchResult(market, year, url, [], None, f"{type(exc).__name__}: {exc}")


def _file_receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "size": int(path.stat().st_size), "sha256": digest.hexdigest()}


def main() -> None:
    args = parse_args()
    end_date = date.fromisoformat(args.end_date)
    if args.daily_overlap_years < 1:
        raise ValueError("--daily-overlap-years must be >= 1")
    output_path = args.output_dir / "tw_corporate_action_reference.parquet"
    summary_path = args.output_dir / "tw_corporate_action_reference.summary.json"
    previous_summary: dict[str, Any] = {}
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            previous_summary = payload if isinstance(payload, dict) else {}
        except (OSError, ValueError):
            previous_summary = {}
    if args.mode == "daily":
        if not output_path.exists() or not bool(previous_summary.get("baseline_established")):
            raise RuntimeError(
                "daily corporate-action update requires a complete baseline; run --mode repair first"
            )
        request_start_year = max(
            int(args.start_year),
            end_date.year - int(args.daily_overlap_years) + 1,
        )
    else:
        request_start_year = int(args.start_year)
    tasks: list[tuple[str, int]] = []
    for market, first_year in (("twse", 2003), ("tpex", 2000)):
        tasks.extend(
            (market, year)
            for year in range(max(request_start_year, first_year), end_date.year + 1)
        )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results: list[FetchResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(_fetch_year, market, year, end_date, args): (market, year)
            for market, year in tasks
        }
        for future in as_completed(futures):
            results.append(future.result())
    failures = [result for result in results if result.error]
    if failures:
        report = [asdict(result) for result in failures]
        raise RuntimeError(f"corporate-action reference requests failed: {report[:10]}")
    rows = [row for result in results for row in result.rows]
    frame = pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "market": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
            "previous_close": pl.Float64,
            "event_type": pl.String,
            "source_url": pl.String,
        }
    )
    if not frame.is_empty():
        conflicts = (
            frame.group_by(["date", "symbol"])
            .agg(pl.col("reference_price").drop_nulls().n_unique().alias("references"))
            .filter(pl.col("references") > 1)
        )
        if conflicts.height:
            raise RuntimeError(f"conflicting official corporate-action references: {conflicts.head(20)}")
        frame = (
            frame.with_columns(pl.col("reference_price").is_not_null().alias("_has_reference"))
            .sort(["date", "symbol", "_has_reference", "market"], descending=[False, False, True, False])
            .unique(["date", "symbol"], keep="first", maintain_order=True)
            .drop("_has_reference")
        )
    if args.mode != "rebuild" and output_path.exists():
        existing = pl.read_parquet(output_path)
        if not existing.is_empty():
            requested_start = date(request_start_year, 1, 1)
            existing_date = pl.col("date").cast(pl.Date, strict=False)
            existing = existing.filter(
                (existing_date < requested_start) | (existing_date > end_date)
            )
            frame = (
                existing
                if frame.is_empty()
                else pl.concat([existing, frame], how="diagonal_relaxed")
            )
            if not frame.is_empty():
                frame = frame.sort(["date", "symbol"]).unique(
                    ["date", "symbol"], keep="last", maintain_order=True
                )
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent, delete=False
    )
    temporary_path = Path(handle.name)
    handle.close()
    try:
        frame.write_parquet(temporary_path, compression="zstd")
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    previous_baseline = bool(previous_summary.get("baseline_established"))
    baseline_established = previous_baseline or (
        args.mode in {"rebuild", "repair"} and request_start_year <= 2000
    )
    summary = {
        "schema_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "mode": args.mode,
        "requested_start_year": request_start_year,
        "coverage_start_year": (
            min(int(previous_summary.get("coverage_start_year", request_start_year)), request_start_year)
            if args.mode != "rebuild" and previous_summary
            else request_start_year
        ),
        "baseline_established": baseline_established,
        "coverage_complete": baseline_established,
        "end_date": str(end_date),
        "request_count": len(results),
        "failure_count": 0,
        "rows": int(frame.height),
        "markets": frame.group_by("market").len().sort("market").to_dicts() if frame.height else [],
        "raw_files": [result.raw_path for result in results if result.raw_path],
        "output_receipt": _file_receipt(output_path),
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"[tw-corporate-actions] requests={len(results)} rows={frame.height} "
        f"output={output_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
