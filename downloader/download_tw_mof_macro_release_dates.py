"""Archive MOF trade/tax release dates without treating current bulk values as vintages."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import polars as pl
import requests
from pypdf import PdfReader
from pypdf.errors import PdfReadError

try:
    from downloader.common import SharedRateLimiter, retry_delay_seconds
    from downloader.download_tw_cbc_fx_release_archive import _save_raw
    from downloader.tw_public_source_lock import source_update_lock
except ImportError:  # direct invocation from downloader/
    from common import SharedRateLimiter, retry_delay_seconds
    from download_tw_cbc_fx_release_archive import _save_raw
    from tw_public_source_lock import source_update_lock


NAME = "mof_macro_release_dates"
BASE = "https://www.mof.gov.tw"
PATH = "/multiplehtml/384fb3077bb349ea973e7fc6f13b6974"
CATEGORIES = {"trade": "STAT_EXP", "tax": "STAT_DOT"}
TITLE_PATTERNS = {
    "trade": re.compile(r"^(\d{2,3})年\s*(\d{1,2})月海關進出口貿易初步統計$"),
    "tax": re.compile(r"^(\d{2,3})年\s*(\d{1,2})月全國賦稅收入初步統計$"),
}


def _fetch(url: str, limiter: SharedRateLimiter) -> bytes:
    if urlparse(url).hostname != "www.mof.gov.tw" or not url.startswith(BASE + PATH):
        raise ValueError(f"out-of-scope MOF URL: {url}")
    for attempt in range(5):
        limiter.wait()
        try:
            with requests.get(url, timeout=(10, 30), stream=True,
                              allow_redirects=False,
                              headers={"User-Agent": "stockAgent-official-release-index/1.0"}) as response:
                if response.status_code in {429, 500, 502, 503, 504}:
                    limiter.defer(retry_delay_seconds(
                        attempt, base=1.0, cap=30.0,
                        retry_after=response.headers.get("Retry-After")))
                    continue
                response.raise_for_status()
                if response.is_redirect:
                    raise ValueError(f"unexpected MOF redirect: {url}")
                if int(response.headers.get("Content-Length") or 0) > 2_000_000:
                    raise ValueError(f"MOF listing exceeds size cap: {url}")
                body = response.content
                if not body or len(body) > 2_000_000:
                    raise ValueError(f"empty/oversized MOF listing: {url}")
                return body
        except requests.RequestException:
            if attempt == 4:
                raise
            limiter.defer(retry_delay_seconds(attempt, base=1.0, cap=30.0))
    raise RuntimeError(f"MOF listing exhausted retries: {url}")


def parse_listing(body: bytes, series: str) -> tuple[list[dict], int, int, int]:
    if series not in CATEGORIES:
        raise ValueError(f"unsupported MOF series: {series}")
    soup = BeautifulSoup(body, "html.parser")
    table = soup.select_one("div.application table.table-list")
    if table is None:
        raise ValueError("MOF listing table is missing")
    headers = [cell.get_text(" ", strip=True) for cell in table.select("thead th")]
    if headers != ["序號", "標題", "發布日期"]:
        raise ValueError(f"MOF listing headings changed: {headers}")
    page_text = soup.select_one("div#pages p")
    match = re.search(r"總共\s*(\d+)\s*頁[，,]\s*(\d+)\s*筆資料", page_text.get_text(" ", strip=True) if page_text else "")
    if match is None:
        raise ValueError("MOF listing lacks pagination receipt")
    rows = []
    listed = table.select("tbody tr")
    for tr in listed:
        link = tr.select_one('td[data-title="標題："] a[href]')
        published_cell = tr.select_one('td[data-title="發布日期："]')
        if link is None or published_cell is None:
            raise ValueError("MOF listing row lacks link/publication date")
        title = link.get_text(" ", strip=True)
        title_match = TITLE_PATTERNS[series].fullmatch(title)
        if title_match is None:
            continue
        year, month = map(int, title_match.groups())
        if not 1 <= month <= 12:
            raise ValueError(f"invalid MOF release period: {title}")
        published = date.fromisoformat(published_cell.get_text(" ", strip=True))
        url = urljoin(BASE, link["href"])
        if (urlparse(url).hostname != "www.mof.gov.tw" or
                not urlparse(url).path.startswith(("/singlehtml/", "/download/"))):
            raise ValueError(f"MOF release link escapes official release pages: {url}")
        rows.append({"series": series, "period": f"{year + 1911:04d}-{month:02d}",
                     "published_on": published.isoformat(), "release_url": url,
                     "title": title})
    return rows, int(match.group(1)), int(match.group(2)), len(listed)


def parse_tax_pdf_release_date(body: bytes, period: str) -> date:
    reader = PdfReader(io.BytesIO(body))
    if not reader.pages:
        raise ValueError("MOF tax release PDF has no pages")
    text = reader.pages[0].extract_text() or ""
    return parse_tax_pdf_text_release_date(text, period)


def parse_tax_pdf_text_release_date(text: str, period: str) -> date:
    roc_year, month = int(period[:4]) - 1911, int(period[-2:])
    if not re.search(rf"{roc_year}\s*年\s*{month}\s*月全國賦稅收入初步統計", text):
        raise ValueError(f"MOF tax PDF does not match period {period}")
    published = re.search(
        r"財政部新聞稿\s*(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日發布",
        text,
    )
    if published is None:
        raise ValueError(f"MOF tax PDF lacks a release date: {period}")
    year, month, day = map(int, published.groups())
    return date(year + 1911, month, day)


def parse_next_tax_release_schedule(body: bytes, *, next_period: str) -> tuple[date, str]:
    reader = PdfReader(io.BytesIO(body))
    text = reader.pages[0].extract_text() or ""
    return parse_next_tax_release_schedule_text(text, next_period=next_period)


def parse_next_tax_release_schedule_text(text: str, *, next_period: str) -> tuple[date, str]:
    match = re.search(
        r"下次發布日期[：:]\s*(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"
        r"(?:\s*(上午|下午)\s*(\d{1,2})\s*時)?",
        text,
    )
    if match is None:
        raise ValueError(f"MOF prior tax PDF lacks next-release schedule for {next_period}")
    year, month, day = map(int, match.group(1, 2, 3))
    scheduled = date(year + 1911, month, day)
    period_year, period_month = map(int, next_period.split("-"))
    expected_year = period_year + (period_month == 12)
    expected_month = 1 if period_month == 12 else period_month + 1
    if (scheduled.year, scheduled.month) != (expected_year, expected_month):
        raise ValueError(f"MOF prior schedule does not match {next_period}: {scheduled}")
    hour = int(match.group(5)) if match.group(5) else 16
    if match.group(4) == "下午" and hour < 12:
        hour += 12
    if not 0 <= hour < 24:
        raise ValueError(f"invalid MOF scheduled release hour: {hour}")
    return scheduled, f"{hour:02d}:00:00"


def _fetch_tax_pdf(url: str, limiter: SharedRateLimiter) -> bytes | None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "service.mof.gov.tw" or not parsed.path.startswith("/public/Data/statistic/news/"):
        raise ValueError(f"out-of-scope MOF tax PDF URL: {url}")
    for attempt in range(5):
        limiter.wait()
        try:
            with requests.get(url, timeout=(10, 45), stream=True, allow_redirects=False,
                              headers={"User-Agent": "stockAgent-official-release-index/1.0"}) as response:
                if response.status_code == 404:
                    return None
                if response.status_code in {429, 500, 502, 503, 504}:
                    limiter.defer(retry_delay_seconds(
                        attempt, base=1.0, cap=30.0,
                        retry_after=response.headers.get("Retry-After")))
                    continue
                response.raise_for_status()
                if response.is_redirect:
                    raise ValueError(f"unexpected MOF tax PDF redirect: {url}")
                if int(response.headers.get("Content-Length") or 0) > 8_000_000:
                    raise ValueError(f"MOF tax PDF exceeds byte cap: {url}")
                body = response.content
                if not body.startswith(b"%PDF") or len(body) > 8_000_000:
                    raise ValueError(f"MOF tax PDF is not a valid PDF: {url}")
                return body
        except requests.RequestException:
            if attempt == 4:
                raise
            limiter.defer(retry_delay_seconds(attempt, base=1.0, cap=30.0))
    raise RuntimeError(f"MOF tax PDF exhausted retries: {url}")


def collect(root: Path, *, request_interval: float = 0.5,
            recent_pages: int = 2) -> dict:
    if request_interval <= 0 or recent_pages < 0:
        raise ValueError("request interval must be positive and recent-pages nonnegative")
    output = root / "supplemental" / f"{NAME}.parquet"
    old = pl.read_parquet(output).to_dicts() if output.is_file() else []
    old_by_key = {(row["series"], row["period"]): row for row in old}
    state_path = root / "state" / f"{NAME}.json"
    try:
        prior_state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        prior_state = {}
    fully_scanned = bool(prior_state.get("full_history_scanned"))
    limiter = SharedRateLimiter(request_interval, name="mof_macro_release_index")
    latest = dict(old_by_key)
    category_receipts: dict[str, dict] = {}
    completed_all = True
    for series, category in CATEGORIES.items():
        index = f"{BASE}{PATH}?categoryCode={category}"
        first = _fetch(index, limiter)
        first_rows, pages, advertised, first_listed = parse_listing(first, series)
        pages_to_read = pages if not old or not fully_scanned or recent_pages == 0 else min(pages, recent_pages)
        completed_all = completed_all and pages_to_read == pages
        listed_total = 0
        matches = []
        for page in range(1, pages_to_read + 1):
            url = index if page == 1 else f"{index}&page={page}"
            body = first if page == 1 else _fetch(url, limiter)
            parsed, observed_pages, observed_advertised, listed = (
                first_rows, pages, advertised, first_listed) if page == 1 else parse_listing(body, series)
            if observed_pages != pages or observed_advertised != advertised:
                raise ValueError(f"MOF {series} pagination changed during scan")
            digest, raw_path = _save_raw(root / "raw" / NAME / series,
                                         f"page-{page:03d}", body)
            listed_total += listed
            observed = datetime.now(timezone.utc).isoformat()
            matches.extend({**row, "index_url": url, "index_sha256": digest,
                            "raw_path": raw_path, "observed_at_utc": observed}
                           for row in parsed)
        if pages_to_read == pages and listed_total != advertised:
            raise ValueError(f"MOF {series} full listing count {listed_total} != {advertised}")
        for row in matches:
            key = row["series"], row["period"]
            previous = latest.get(key)
            if previous is None or row["published_on"] < previous["published_on"]:
                latest[key] = row
        category_receipts[series] = {"pages": pages, "pages_read": pages_to_read,
                                     "advertised_rows": advertised,
                                     "matched_releases": len(matches)}
    pdf_fallback = {"recovered": 0, "not_found": [], "failures": [],
                    "scheduled_inferences": [], "attempted": 0}
    # The MOF news category omits a 2018-19 tax block. Original, dated press
    # PDFs still exist at the government's stable period path for most gaps.
    # Recheck absent paths on full weekly scans; never invent an archive date.
    if completed_all:
        tax_periods = sorted(period for series, period in latest if series == "tax")
        if tax_periods:
            start_year, start_month = map(int, tax_periods[0].split("-"))
            end_year, end_month = map(int, tax_periods[-1].split("-"))
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    if (year, month) < (start_year, start_month) or (year, month) > (end_year, end_month):
                        continue
                    period = f"{year:04d}-{month:02d}"
                    if ("tax", period) in latest:
                        continue
                    code = f"{year - 1911:03d}{month:02d}"
                    url = f"https://service.mof.gov.tw/public/Data/statistic/news/{code}/{code}-news.pdf"
                    pdf_fallback["attempted"] += 1
                    try:
                        body = _fetch_tax_pdf(url, limiter)
                    except (ValueError, requests.RequestException) as exc:
                        pdf_fallback["failures"].append({
                            "period": period, "error": f"{type(exc).__name__}: {exc}"
                        })
                        continue
                    if body is None:
                        pdf_fallback["not_found"].append(period)
                        continue
                    try:
                        published = parse_tax_pdf_release_date(body, period)
                    except (ValueError, PdfReadError) as exc:
                        pdf_fallback["failures"].append({
                            "period": period, "error": f"{type(exc).__name__}: {exc}"
                        })
                        continue
                    digest = hashlib.sha256(body).hexdigest()
                    raw_path = root / "raw" / NAME / "tax_pdfs" / f"{code}-{digest[:16]}.pdf"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    if raw_path.exists():
                        if hashlib.sha256(raw_path.read_bytes()).hexdigest() != digest:
                            raise RuntimeError(f"MOF tax PDF cache checksum mismatch: {raw_path}")
                    else:
                        temporary_pdf = raw_path.with_suffix(".pdf.tmp")
                        temporary_pdf.write_bytes(body)
                        os.replace(temporary_pdf, raw_path)
                    latest[("tax", period)] = {
                        "series": "tax", "period": period,
                        "published_on": published.isoformat(), "release_url": url,
                        "title": f"{year - 1911}年{month}月全國賦稅收入初步統計",
                        "index_url": url, "index_sha256": digest,
                        "raw_path": str(raw_path),
                        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                    }
                    pdf_fallback["recovered"] += 1
        for period in pdf_fallback["not_found"]:
            year, month = map(int, period.split("-"))
            prior = f"{year - 1:04d}-12" if month == 1 else f"{year:04d}-{month - 1:02d}"
            previous = latest.get(("tax", prior))
            if previous is None or not str(previous.get("raw_path") or "").endswith(".pdf"):
                continue
            prior_path = Path(str(previous["raw_path"]))
            if not prior_path.is_file():
                continue
            body = prior_path.read_bytes()
            if hashlib.sha256(body).hexdigest() != previous["index_sha256"]:
                raise RuntimeError(f"MOF prior tax PDF checksum mismatch: {prior_path}")
            try:
                scheduled, scheduled_clock = parse_next_tax_release_schedule(
                    body, next_period=period
                )
            except (ValueError, PdfReadError):
                continue
            pdf_fallback["scheduled_inferences"].append({
                "period": period, "scheduled_on": scheduled.isoformat(),
                "scheduled_clock_taipei": scheduled_clock,
                "evidence_url": previous["release_url"],
                "evidence_sha256": previous["index_sha256"],
            })
    rows = [latest[key] for key in sorted(latest)]
    if not rows:
        raise ValueError("MOF indexes yielded no trade/tax releases")
    output.parent.mkdir(parents=True, exist_ok=True)
    if rows != old:
        temporary = output.with_suffix(".parquet.tmp")
        pl.DataFrame(rows).write_parquet(temporary, compression="zstd")
        os.replace(temporary, output)
    state = {"dataset": NAME, "status": "complete" if fully_scanned or completed_all else "partial",
             "full_history_scanned": fully_scanned or completed_all,
             "distinct_release_periods": len(rows), "categories": category_receipts,
             "tax_pdf_fallback": pdf_fallback,
             "earliest_period_by_series": {
                 series: min(row["period"] for row in rows if row["series"] == series)
                 for series in CATEGORIES
             },
             "latest_period_by_series": {
                 series: max(row["period"] for row in rows if row["series"] == series)
                 for series in CATEGORIES
             },
             "latest_published_on": max(row["published_on"] for row in rows),
             "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, state_path)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--recent-pages", type=int, default=2,
                        help="Zero forces a complete scan; first run always scans all pages.")
    parser.add_argument("--source-update-lock-held", action="store_true")
    args = parser.parse_args()
    root = args.output_dir.resolve()
    lock_path = root / "state" / "locks" / f"{NAME}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with source_update_lock(root, already_held=args.source_update_lock_held):
        with lock_path.open("a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("MOF release index collector is already active") from exc
            summary = collect(root, request_interval=args.request_interval,
                              recent_pages=args.recent_pages)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
