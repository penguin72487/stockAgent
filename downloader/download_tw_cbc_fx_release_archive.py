"""Archive CBC foreign-reserve press releases as dated, original-value vintages.

The current-value CBC bulk table is not a point-in-time history.  This job
keeps the original HTML, its checksum, and only values stated by that release.
An article's date is known; its intra-day publication time usually is not.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
import unicodedata
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import requests

try:
    from downloader.common import SharedRateLimiter, retry_delay_seconds
    from downloader.release_archive_io import write_release_rows_if_changed
except ImportError:  # direct invocation from downloader/
    from common import SharedRateLimiter, retry_delay_seconds
    from release_archive_io import write_release_rows_if_changed

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


BASE = "https://www.cbc.gov.tw"
OUTPUT_NAME = "cbc_fx_reserve_release_vintages"
LIST_URL = BASE + "/tw/lp-302-1-{page}-60.html"
LEGACY_LIST_URL = BASE + "/tw/lp-302-1-{page}-20.html"
LIST_PAGE_SIZE = 60
MAX_BODY_BYTES = 2_000_000


class SourceAccessBlocked(RuntimeError):
    """A source challenge is not a quota that should be retried blindly."""


def _reject_source_error_page(body: bytes) -> None:
    # CBC's upstream proxy has returned this HTML error with HTTP 200. It is
    # neither a release nor evidence that the official listing has changed.
    if b"this web server can't be reached" in body.lower():
        raise SourceAccessBlocked("official CBC host returned an upstream error page")


PERIOD_RE = re.compile(
    r"(?P<year>\d{2,4}|[零〇一二三四五六七八九十百]+)年\s*"
    r"(?P<month>\d{1,2}|[零〇一二三四五六七八九十]+)月(?:底|末)?外匯存底"
)
YEAR_END_RE = re.compile(r"(?P<year>\d{2,4}|[零〇一二三四五六七八九十百]+)年底外匯存底")
MONTH_ONLY_RE = re.compile(r"(?:\d{1,2}|[零〇一二三四五六七八九十]+)月(?:底|末)?外匯存底")
GENERIC_RESERVE_TITLE_RE = re.compile(r"(?:^|[（(])外匯存底[）)]?$")
VALUE_RE = re.compile(
    r"(?:我國)?外匯存底(?:之)?(?:金額)?\s*(?:為|達|計|有)?\s*"
    r"(?P<value>[\d,]+(?:\.\d+)?)\s*億美元"
)
CHINESE_VALUE_RE = re.compile(
    r"(?:我國)?外匯存底(?:之)?(?:金額)?\s*(?:為|達|計|有)?\s*"
    r"(?P<yi>[零〇一二三四五六七八九兩十百千]+)億"
    r"(?:(?P<wan>[零〇一二三四五六七八九兩十百千]+)萬)?美元"
)
CHINESE_DIGITS = {"零": 0, "〇": 0, "一": 1, "二": 2, "三": 3, "四": 4,
                  "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "兩": 2}
SMALL_UNITS = {"十": 10, "百": 100, "千": 1000}
_THREAD_LOCAL = threading.local()


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "stockAgent-official-release-archive/1.0"})
        _THREAD_LOCAL.session = session
    return session


def _chinese_int(raw: str) -> int:
    if raw.isdigit():
        return int(raw)
    if all(character in CHINESE_DIGITS for character in raw):
        value = 0
        for character in raw:
            value = value * 10 + CHINESE_DIGITS[character]
        return value
    value = 0
    digit = 0
    for character in raw:
        if character in CHINESE_DIGITS:
            digit = CHINESE_DIGITS[character]
        else:
            value += (digit or 1) * SMALL_UNITS[character]
            digit = 0
    return value + digit


def _period(title: str) -> str | None:
    match = PERIOD_RE.search(title)
    if match is None:
        year_end = YEAR_END_RE.search(title)
        if year_end is None:
            return None
        year = _chinese_int(year_end["year"])
        year += 1911 if year < 1911 else 0
        return f"{year:04d}-12"
    year = _chinese_int(match["year"])
    year += 1911 if year < 1911 else 0
    month = _chinese_int(match["month"])
    return f"{year:04d}-{month:02d}" if 1 <= month <= 12 else None


@dataclass(frozen=True)
class ListingPage:
    rows: list[dict[str, str]]
    page: int
    total_pages: int
    total_rows: int | None
    raw_rows: int
    page_size: int | None


def _parse_listing_page(content: bytes) -> ListingPage:
    _reject_source_error_page(content)
    soup = BeautifulSoup(content, "html.parser")
    rows: list[dict[str, str]] = []
    raw_rows = 0
    for item in soup.select("li"):
        time_tag = item.find("time")
        if time_tag is not None:
            raw_rows += 1
        link = item.find("a", href=re.compile(r"^/tw/cp-302-"))
        if time_tag is None or link is None:
            continue
        title = link.get_text(" ", strip=True)
        period = _period(title)
        if period is None and not (
            MONTH_ONLY_RE.search(title) or GENERIC_RESERVE_TITLE_RE.search(title)
        ):
            continue
        published_on = time_tag.get_text(strip=True)
        datetime.strptime(published_on, "%Y-%m-%d")
        url = urljoin(BASE, link["href"])
        if urlparse(url).hostname != "www.cbc.gov.tw":
            raise ValueError(f"unexpected CBC release host: {url}")
        rows.append({"period": period or "", "published_on": published_on,
                     "title": title, "release_url": url})
    pagination = soup.select_one(".total")
    page_text = (pagination or soup).get_text(" ", strip=True)
    page_match = re.search(r"第\s*(\d+)\s*/\s*(\d+)\s*頁", page_text)
    if page_match is None:
        raise ValueError("CBC listing lacks a verifiable page count")
    total_match = re.search(r"共\s*(\d+)\s*筆資料", page_text)
    selected = soup.select_one("#PageSize option[selected]")
    selected_size = str(selected.get("value") or "") if selected else ""
    return ListingPage(
        rows=rows,
        page=int(page_match[1]),
        total_pages=int(page_match[2]),
        total_rows=int(total_match[1]) if total_match else None,
        raw_rows=raw_rows,
        page_size=int(selected_size) if selected_size.isdigit() else None,
    )


def parse_listing(content: bytes) -> tuple[list[dict[str, str]], int]:
    page = _parse_listing_page(content)
    return page.rows, page.total_pages


def parse_detail(content: bytes, listed: dict[str, str]) -> tuple[str, float | None, str | None]:
    soup = BeautifulSoup(content, "html.parser")
    title = soup.select_one("h2.title")
    published = soup.select_one(".publish_time time")
    article = soup.select_one("section.cp")
    if title is None or title.get_text(" ", strip=True) != listed["title"]:
        raise ValueError(f"CBC release title mismatch: {listed['release_url']}")
    if published is None or published.get_text(strip=True) != listed["published_on"]:
        raise ValueError(f"CBC release date mismatch: {listed['release_url']}")
    if article is None:
        raise ValueError(f"CBC release body missing: {listed['release_url']}")
    text = unicodedata.normalize("NFKC", article.get_text(" ", strip=True))
    article_period = _period(text[:800])
    period = article_period or listed["period"]
    matches = [float(match["value"].replace(",", "")) for match in VALUE_RE.finditer(text)]
    if not matches:
        matches = [
            _chinese_int(match["yi"]) + (_chinese_int(match["wan"]) / 10_000 if match["wan"] else 0)
            for match in CHINESE_VALUE_RE.finditer(text)
        ]
    # The main statement precedes comparison notes. A missing period or
    # unambiguous amount remains raw-only, never a fabricated vintage.
    if not period:
        return "", None, "release_period_not_found"
    if not matches:
        return period, None, "headline_amount_not_found"
    return period, matches[0], None


def _fetch(url: str, limiter: SharedRateLimiter) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "www.cbc.gov.tw":
        raise ValueError(f"out-of-scope CBC URL: {url}")
    for attempt in range(5):
        limiter.wait()
        try:
            with _session().get(url, timeout=(10, 30), stream=True,
                                allow_redirects=False) as response:
                if response.status_code in {429, 500, 502, 503, 504}:
                    if (
                        response.status_code == 429
                        and not response.headers.get("Retry-After")
                        and "cloudflare" in response.headers.get("Server", "").lower()
                    ):
                        raise SourceAccessBlocked(
                            f"official CBC host returned Cloudflare HTTP 429 without Retry-After: {url}"
                        )
                    limiter.defer(retry_delay_seconds(
                        attempt, base=1.0, cap=30.0,
                        retry_after=response.headers.get("Retry-After")))
                    continue
                if 300 <= response.status_code < 400:
                    # CBC occasionally returns a transient redirect for a valid
                    # listing URL. Never follow it: the target may be an error
                    # or challenge page, not the requested official release.
                    # Retrying the original URL preserves its provenance.
                    if attempt == 4:
                        raise ValueError(
                            f"unexpected CBC redirect after retries: {url} "
                            f"(HTTP {response.status_code})"
                        )
                    limiter.defer(retry_delay_seconds(
                        attempt, base=1.0, cap=30.0,
                        retry_after=response.headers.get("Retry-After")))
                    continue
                response.raise_for_status()
                if int(response.headers.get("Content-Length") or 0) > MAX_BODY_BYTES:
                    raise ValueError(f"CBC body exceeds byte cap: {url}")
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_content(chunk_size=128_000):
                    total += len(chunk)
                    if total > MAX_BODY_BYTES:
                        raise ValueError(f"CBC body exceeds byte cap: {url}")
                    chunks.append(chunk)
                body = b"".join(chunks)
                if not body:
                    raise ValueError(f"empty CBC body: {url}")
                _reject_source_error_page(body)
                return body
        except requests.RequestException:
            if attempt == 4:
                raise
            limiter.defer(retry_delay_seconds(attempt, base=1.0, cap=30.0))
    raise RuntimeError(f"CBC release exhausted retries: {url}")


def _save_raw(directory: Path, prefix: str, body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    path = directory / f"{prefix}-{digest[:16]}.html"
    directory.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"CBC cached release checksum mismatch: {path}")
    else:
        temporary = path.with_suffix(".html.tmp")
        temporary.write_bytes(body)
        os.replace(temporary, path)
    return digest, str(path)


def _cached(directory: Path, prefix: str, *, listing: bool = False) -> bytes | None:
    paths = sorted(directory.glob(f"{prefix}-*.html"), key=lambda path: path.stat().st_mtime_ns,
                   reverse=True)
    for path in paths:
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest()[:16] != path.stem.removeprefix(f"{prefix}-"):
            raise RuntimeError(f"CBC cached release checksum mismatch: {path}")
        if listing:
            try:
                parse_listing(body)
            except (ValueError, SourceAccessBlocked):
                # Retain the raw bytes for audit but never let a later cached
                # run mistake a proxy error for an official listing.
                continue
        return body
    return None


def _listing_layout(root: Path, *, cached_only: bool) -> tuple[str, int, str]:
    """Keep 20-row legacy originals usable without mixing their page numbers.

    A partial 60-row cache must fail closed instead of silently combining it
    with 20-row pages. New online runs always verify the full 60-row listing.
    """
    directory = root / "raw" / OUTPUT_NAME / "list"
    if cached_only and not any(directory.glob("page-0060-0001-*.html")):
        return LEGACY_LIST_URL, 20, "page-{page:04d}"
    return LIST_URL, LIST_PAGE_SIZE, "page-0060-{page:04d}"


@contextmanager
def _writer_lock(root: Path):
    if fcntl is None:
        raise RuntimeError("CBC archive requires a POSIX writer lock")
    path = root / "state" / "locks" / f"{OUTPUT_NAME}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another CBC release archive writer is active") from exc
        yield


def _write_state(root: Path, state: dict[str, object]) -> None:
    path = root / "state" / f"{OUTPUT_NAME}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _collect_one(listed: dict[str, str], root: Path, limiter: SharedRateLimiter,
                 *, refresh: bool, offline_cache_only: bool = False) -> dict[str, object]:
    release_id = Path(urlparse(listed["release_url"]).path).stem
    directory = root / "raw" / OUTPUT_NAME / "detail" / release_id
    cached_body = None if refresh else _cached(directory, "article")
    body = cached_body
    if body is None:
        if offline_cache_only:
            raise FileNotFoundError(f"no cached original CBC article: {listed['release_url']}")
        body = _fetch(listed["release_url"], limiter)
    try:
        period, value, parse_warning = parse_detail(body, listed)
    except ValueError:
        if refresh or offline_cache_only or cached_body is None:
            raise
        body = _fetch(listed["release_url"], limiter)
        period, value, parse_warning = parse_detail(body, listed)
    digest, path = _save_raw(directory, "article", body)
    return {**listed, "period": period,
            "listing_period": listed["period"],
            "period_corrected_from_body": bool(listed["period"] and period != listed["period"]),
            "metric": "fx_reserves_usd_100m" if value is not None else None,
            "value": value, "value_evidence": "original_press_release_text" if value is not None else "raw_release_only",
            "published_time_precision": "official_date_only", "parse_warning": parse_warning,
            "html_sha256": digest, "html_path": path,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds")}


def collect(root: Path, *, workers: int = 8, request_interval: float = 0.1,
            refresh_recent: int = 3, offline_cache_only: bool = False,
            cached_list_pages: bool = False) -> dict[str, object]:
    if not 1 <= workers <= 32 or not 0.1 <= request_interval <= 60 or refresh_recent < 0:
        raise ValueError("invalid worker, request interval, or refresh setting")
    limiter = SharedRateLimiter(request_interval, name="cbc-release-archive")
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running", "phase": "discovering",
                        "started_at_utc": started_at, "completed_releases": 0,
                        "total_releases": None, "estimated_seconds_remaining": None})
    listed: dict[str, dict[str, str]] = {}
    list_receipts: list[dict[str, object]] = []
    list_url, listing_page_size, page_prefix = _listing_layout(
        root, cached_only=offline_cache_only or cached_list_pages
    )
    discovery_started = time.monotonic()
    def fetch_page(page: int) -> tuple[int, str, str, ListingPage]:
        url = list_url.format(page=page)
        prefix = page_prefix.format(page=page)
        body = (
            _cached(root / "raw" / OUTPUT_NAME / "list", prefix, listing=True)
            if offline_cache_only or cached_list_pages else _fetch(url, limiter)
        )
        if body is None:
            raise FileNotFoundError(f"no cached CBC listing page {page}")
        listing = _parse_listing_page(body)
        digest, path = _save_raw(root / "raw" / OUTPUT_NAME / "list", prefix, body)
        return page, digest, path, listing

    first = fetch_page(1)
    total_pages = first[3].total_pages
    if not 1 <= total_pages <= 1000:
        raise ValueError(f"CBC reported implausible page count: {total_pages}")
    pages = [first]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_page, page): page for page in range(2, total_pages + 1)}
        for future in as_completed(futures):
            try:
                pages.append(future.result())
            except SourceAccessBlocked:
                pool.shutdown(wait=False, cancel_futures=True)
                raise
            completed_pages = len(pages)
            if completed_pages % 20 == 0 or completed_pages == total_pages:
                elapsed = max(time.monotonic() - started, 0.001)
                _write_state(root, {
                    "dataset": OUTPUT_NAME, "status": "running", "phase": "discovering",
                    "started_at_utc": started_at, "completed_pages": completed_pages,
                    "total_pages": total_pages, "completed_releases": 0,
                    "total_releases": None,
                    "estimated_seconds_remaining": round(
                        (total_pages - completed_pages) * elapsed / completed_pages
                    ),
                })
    total_rows = first[3].total_rows
    if total_rows is None or total_rows < 1:
        raise ValueError("CBC listing lacks a verifiable total row count")
    if (total_rows + listing_page_size - 1) // listing_page_size != total_pages:
        raise ValueError("CBC listing page count contradicts its total row count")
    for page, digest, path, listing in sorted(pages):
        if listing.page != page or listing.total_pages != total_pages:
            raise ValueError("CBC listing page identity or count changed during discovery; retry")
        if listing.total_rows != total_rows or listing.page_size != listing_page_size:
            raise ValueError("CBC listing total rows or page size changed during discovery; retry")
        expected_raw_rows = min(listing_page_size, total_rows - (page - 1) * listing_page_size)
        if listing.raw_rows != expected_raw_rows:
            raise ValueError(f"CBC listing page {page} has {listing.raw_rows} of {expected_raw_rows} rows")
        url = list_url.format(page=page)
        list_receipts.append({"page": page, "url": url, "sha256": digest,
                              "path": path, "raw_rows": listing.raw_rows,
                              "release_rows": len(listing.rows)})
        for row in listing.rows:
            previous = listed.get(row["release_url"])
            if previous is not None and previous != row:
                raise ValueError(f"CBC listing conflict for {row['release_url']}")
            listed[row["release_url"]] = row
    discovery_seconds = round(time.monotonic() - discovery_started, 3)
    ordered = sorted(listed.values(), key=lambda item: (item["published_on"], item["release_url"]))
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running", "phase": "downloading",
                        "started_at_utc": started_at, "completed_releases": 0,
                        "total_releases": len(ordered), "estimated_seconds_remaining": None})
    results: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    newest = {row["release_url"] for row in ordered[-refresh_recent:]} if refresh_recent else set()
    last_progress = time.monotonic()
    detail_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_collect_one, row, root, limiter,
                               refresh=row["release_url"] in newest,
                               offline_cache_only=offline_cache_only): row for row in ordered}
        for future in as_completed(futures):
            row = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                failures.append({"release_url": row["release_url"], "error": f"{type(exc).__name__}: {exc}"})
            completed = len(results) + len(failures)
            now = time.monotonic()
            if now - last_progress >= 5 or completed == len(ordered):
                _write_state(root, {"dataset": OUTPUT_NAME, "status": "running",
                                    "phase": "downloading", "started_at_utc": started_at,
                                    "completed_releases": completed, "total_releases": len(ordered),
                                    "successful_releases": len(results), "failed_releases": len(failures),
                                    "estimated_seconds_remaining": round(
                                        (len(ordered) - completed) * (now - started) / completed
                                    ) if completed else None})
                last_progress = now
    detail_seconds = round(time.monotonic() - detail_started, 3)
    results.sort(key=lambda row: (str(row["published_on"]), str(row["release_url"])))
    periods = {str(row["period"]) for row in results if row["metric"] is not None}
    missing_periods: list[str] = []
    if periods:
        year, month = map(int, min(periods).split("-"))
        through = max(periods)
        while f"{year:04d}-{month:02d}" <= through:
            period = f"{year:04d}-{month:02d}"
            if period not in periods:
                missing_periods.append(period)
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    complete = (
        not offline_cache_only and not cached_list_pages and not failures
        and len(results) == len(ordered) and not missing_periods
    )
    summary: dict[str, object] = {
        "dataset": OUTPUT_NAME, "status": "complete" if complete else "degraded",
        "offline_cache_only": offline_cache_only,
        "cached_list_pages": cached_list_pages,
        "listing_page_size": listing_page_size,
        "listing_pages": total_pages,
        "stage_seconds": {"listing_discovery": discovery_seconds,
                          "detail_collection": detail_seconds},
        "complete": complete, "registered_releases": len(ordered), "saved_releases": len(results),
        "headline_values": sum(row["metric"] is not None for row in results),
        "distinct_periods": len(periods), "earliest_period": min(periods) if periods else None,
        "latest_period": max(periods) if periods else None, "missing_periods": missing_periods,
        "failures": failures, "listing_receipts": list_receipts,
        "started_at_utc": started_at, "elapsed_seconds": round(time.monotonic() - started, 3),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    # A failed detail fetch must not replace a previously complete archive
    # with a subset. Raw successful responses remain available for resume.
    if results and not failures:
        write_started = time.monotonic()
        destination = root / f"{OUTPUT_NAME}.parquet"
        parquet_sha256, changed = write_release_rows_if_changed(
            destination, results, identity_columns=("release_url",)
        )
        summary["parquet_path"] = str(destination)
        summary["parquet_sha256"] = parquet_sha256
        summary["parquet_changed"] = changed
        summary["stage_seconds"]["parquet_proof_write"] = round(
            time.monotonic() - write_started, 3
        )
    _write_state(root, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-interval", type=float, default=0.1,
                        help="Host-global interval; no numeric CBC limit is documented.")
    parser.add_argument("--refresh-recent", type=int, default=3)
    parser.add_argument("--offline-cache-only", action="store_true",
                        help="Reparse checksum-verified saved pages without any network requests.")
    parser.add_argument("--cached-list-pages", action="store_true",
                        help="Reuse the saved official index and fetch missing articles only; this cannot certify latest listing freshness.")
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    with _writer_lock(output_dir):
        try:
            summary = collect(output_dir, workers=args.workers,
                              request_interval=args.request_interval,
                              refresh_recent=0 if args.offline_cache_only else args.refresh_recent,
                              offline_cache_only=args.offline_cache_only,
                              cached_list_pages=args.cached_list_pages)
        except Exception as exc:
            state_path = output_dir / "state" / f"{OUTPUT_NAME}.json"
            try:
                previous = json.loads(state_path.read_text(encoding="utf-8"))
                if not isinstance(previous, dict):
                    previous = {}
            except (OSError, ValueError):
                previous = {}
            _write_state(output_dir, {
                **previous, "dataset": OUTPUT_NAME, "status": "degraded", "complete": False,
                "estimated_seconds_remaining": None,
                "error": f"{type(exc).__name__}: {exc}",
                "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            })
            raise
    print(json.dumps({key: value for key, value in summary.items()
                      if key not in {"listing_receipts", "failures"}}, ensure_ascii=False))
    if summary["failures"]:
        print(json.dumps(summary["failures"][:10], ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
