"""Archive dated CBC money-supply announcements without backdating revised bulk data.

Only the M1B/M2 year-on-year figures explicitly stated in each release are
extracted.  The headline does not establish the historical level of either
aggregate, and a date-only posting is not presumed to precede the 09:00 open.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import time
import unicodedata
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import polars as pl
try:
    from downloader.common import SharedRateLimiter
    from downloader.download_tw_cbc_fx_release_archive import (
        BASE, LIST_URL, LIST_PAGE_SIZE, ListingPage, _fetch, _save_raw, _cached, _chinese_int,
        SourceAccessBlocked, _reject_source_error_page,
        _validate_listing_page as _validate_cbc_listing_page,
    )
    from downloader.release_archive_io import (
        read_release_resume_state, write_release_rows_if_changed, write_release_state,
    )
except ImportError:  # direct invocation from downloader/
    from common import SharedRateLimiter
    from download_tw_cbc_fx_release_archive import (
        BASE, LIST_URL, LIST_PAGE_SIZE, ListingPage, _fetch, _save_raw, _cached, _chinese_int,
        SourceAccessBlocked, _reject_source_error_page,
        _validate_listing_page as _validate_cbc_listing_page,
    )
    from release_archive_io import (
        read_release_resume_state, write_release_rows_if_changed, write_release_state,
    )

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


OUTPUT_NAME = "cbc_money_release_vintages"
TITLE_RE = re.compile(
    r"(?:民國)?(?P<year>\d{2,3}|[零〇一二三四五六七八九十百]+)年\s*"
    r"(?P<month>\d{1,2}|[零〇一二三四五六七八九十]+)月(?:金融情況|份?貨幣供給額)"
)
BODY_PERIOD_RE = re.compile(
    r"金融情況(?:民國)?(?P<year>\d{2,3}|[零〇一二三四五六七八九十百]+)年"
    r"(?P<month>\d{1,2}|[零〇一二三四五六七八九十]+)月"
)
PAIR_RE = re.compile(
    r"M1B(?:及|與|和|、)M2年增率[^。；;]{0,85}?分別"
    r"(?:略|微)?(?:上升|下降|回升|升|降)?(?:為|至)"
    r"(?P<m1b>負?-?\d+(?:\.\d+)?)%(?:及|與|和|、|,)"
    r"(?P<m2>負?-?\d+(?:\.\d+)?)%"
)
TRIPLE_RE = re.compile(
    r"M1A(?:、|,|及|與)M1B(?:及|與|和|、|,)M2年增率分別為"
    r"負?-?\d+(?:\.\d+)?%(?:、|,|及|與|和)"
    r"(?P<m1b>負?-?\d+(?:\.\d+)?)%(?:、|,|及|與|和)"
    r"(?P<m2>負?-?\d+(?:\.\d+)?)%"
)
SHARED_YOY_RE = re.compile(
    r"M1B(?:及|與|和|、)M2月增率(?:(?!累計).){0,95}?"
    r"年增率(?:則)?分別(?:上升|下降)?(?:為|至)"
    r"(?P<m1b>負?-?\d+(?:\.\d+)?)%(?:及|與|和|、|,)"
    r"(?P<m2>負?-?\d+(?:\.\d+)?)%"
)
SEPARATE_YOY_RE = {
    metric: re.compile(
        rf"{aggregate}年增率[^。；;]{{0,105}}?"
        r"(?:則|亦)?(?:微|略)?(?:上升|下降|回升|續升|升|降)?(?:為|至)"
        r"(?P<value>負?-?\d+(?:\.\d+)?)%"
    )
    for metric, aggregate in (("m1b_yoy_pct", "M1B"), ("m2_yoy_pct", "M2"))
}
M2_INTERVENING_YOY_RE = re.compile(
    r"M2[^。；;]{0,95}?年增率(?:則|亦)?(?:僅)?(?:略|微)?"
    r"(?:上升|下降|回升|升|降)?(?:為|至)"
    r"(?P<value>負?-?\d+(?:\.\d+)?)%"
)


def _percentage(raw: str) -> float:
    value = -float(raw[1:]) if raw.startswith("負") else float(raw)
    if not -100 <= value <= 100:
        raise ValueError(f"implausible CBC yearly growth percentage: {raw}")
    return value


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
        match = TITLE_RE.search(title)
        published_on = time_tag.get_text(strip=True)
        published_date = datetime.strptime(published_on, "%Y-%m-%d").date()
        if match is not None:
            year = _chinese_int(match["year"])
            year += 1911 if year < 1911 else 0
            month = _chinese_int(match["month"])
        elif (normalized := unicodedata.normalize("NFKC", title)) == "金融情況" or (
            normalized.startswith(("新聞稿", "新聞發佈", "新聞發布"))
            and normalized.endswith("金融情況)")
        ):
            # Early original headlines omit the subject period. This is only
            # a candidate; parse_detail must independently validate the
            # explicit period inside the article before it can enter data.
            year = published_date.year if published_date.month > 1 else published_date.year - 1
            month = published_date.month - 1 if published_date.month > 1 else 12
        else:
            continue
        if not 1 <= month <= 12:
            raise ValueError(f"invalid CBC money-supply period: {title}")
        url = urljoin(BASE, link["href"])
        if urlparse(url).hostname != "www.cbc.gov.tw":
            raise ValueError(f"unexpected CBC release host: {url}")
        rows.append({"period": f"{year:04d}-{month:02d}",
                     "published_on": published_on, "title": title,
                     "release_url": url})
    pagination = soup.select_one(".total")
    page_text = (pagination or soup).get_text(" ", strip=True)
    match = re.search(r"第\s*(\d+)\s*/\s*(\d+)\s*頁", page_text)
    if match is None:
        raise ValueError("CBC listing lacks a verifiable page count")
    total_match = re.search(r"共\s*(\d+)\s*筆資料", page_text)
    selected = soup.select_one("#PageSize option[selected]")
    selected_size = str(selected.get("value") or "") if selected else ""
    return ListingPage(
        rows=rows, page=int(match[1]), total_pages=int(match[2]),
        total_rows=int(total_match[1]) if total_match else None,
        raw_rows=raw_rows, page_size=int(selected_size) if selected_size.isdigit() else None,
    )


def parse_listing(content: bytes) -> tuple[list[dict[str, str]], int]:
    """Retain the historical parser API; collection requires the full proof."""
    listing = _parse_listing_page(content)
    return listing.rows, listing.total_pages


def _validate_listing_page(
    listing: ListingPage, requested_page: int, *, expected: ListingPage | None = None,
) -> None:
    """Prove raw page coverage before filtering monetary-policy releases."""
    _validate_cbc_listing_page(listing, requested_page, page_size=LIST_PAGE_SIZE, expected=expected)


def parse_detail(content: bytes, listed: dict[str, str]) -> tuple[dict[str, float], str | None]:
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
    if TITLE_RE.search(listed["title"]) is None:
        body_period = BODY_PERIOD_RE.search(re.sub(r"\s+", "", text))
        if body_period is None:
            raise ValueError(f"undated headline lacks explicit body period: {listed['release_url']}")
        year = _chinese_int(body_period["year"])
        year += 1911 if year < 1911 else 0
        month = _chinese_int(body_period["month"])
        if f"{year:04d}-{month:02d}" != listed["period"]:
            raise ValueError(f"inferred and original body periods disagree: {listed['release_url']}")
    # Match the current-month release paragraph, not later comparison notes.
    first_section = text.split("貨幣總計數", 1)[-1]
    first_section = re.split(r"準備貨幣|直接金融與間接金融|存款及放款與投資", first_section, 1)[0]
    compact = re.sub(r"\s+", "", first_section)
    match = TRIPLE_RE.search(compact)
    if match is None:
        # Never treat a three-aggregate headline as a two-aggregate headline.
        pair_text = re.sub(r"M1A(?:、|,|及|與)M1B", "", compact)
        match = PAIR_RE.search(pair_text) or SHARED_YOY_RE.search(pair_text)
    if match is not None:
        return {"m1b_yoy_pct": _percentage(match["m1b"]),
                "m2_yoy_pct": _percentage(match["m2"])}, None
    separate = {name: pattern.search(compact) for name, pattern in SEPARATE_YOY_RE.items()}
    if separate["m2_yoy_pct"] is None:
        separate["m2_yoy_pct"] = M2_INTERVENING_YOY_RE.search(compact)
    if all(separate.values()):
        return {name: _percentage(found["value"]) for name, found in separate.items()}, None
    # A flattened HTML table may place current month, cumulative average, and
    # monthly growth in different column orders. Guessing by position caused
    # silent label errors; text evidence is required until header-aware table
    # extraction is implemented and validated per schema.
    return {}, "headline_yoy_not_found"


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
            raise RuntimeError("another CBC money release writer is active") from exc
        yield


def _write_state(root: Path, state: dict[str, object]) -> None:
    write_release_state(root, OUTPUT_NAME, state)


def _verified_resume_rows(root: Path, prior: dict[str, object]) -> list[dict[str, object]]:
    """Reverify pinned bytes, not the latest failed/running status, before reuse.

    The enclosing collector writer lock excludes cooperative archive writers.
    Decode the exact verified Parquet bytes, never reopen the path after hashing.
    Mixed old/new listing receipts are legal for recent-page scans and do not
    claim that all pages were downloaded at the latest polling time.
    """
    root = root.resolve()
    verified: dict[Path, str] = {}

    def verify(path: Path, digest: object, *, capture: bool = False) -> bytes | None:
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("resume proof lacks SHA-256")
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root):
            raise ValueError("resume proof path escapes archive root")
        if resolved in verified and verified[resolved] != digest:
            raise ValueError("resume proof has conflicting hashes")
        if resolved in verified and not capture:
            return None
        before = path.stat()
        with path.open("rb") as handle:
            if capture:
                body = handle.read()
                actual = hashlib.sha256(body).hexdigest()
            else:
                body = None
                actual = hashlib.file_digest(handle, "sha256").hexdigest()
        after = path.stat()
        def identity(st):
            return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns
        if actual != digest or identity(before) != identity(after) or path.resolve() != resolved:
            raise ValueError("resume source changed or SHA-256 differs from receipt")
        verified[resolved] = digest
        return body

    def raw_path(value: object, subtree: str) -> Path:
        if not isinstance(value, str) or ".." in Path(value).parts:
            raise ValueError("invalid resume raw path")
        path = Path(value)
        path = path if path.is_absolute() else root / path
        if not path.resolve(strict=True).is_relative_to(root / "raw" / OUTPUT_NAME / subtree):
            raise ValueError("resume raw path escapes dataset subtree")
        return path

    body = verify(root / f"{OUTPUT_NAME}.parquet", prior.get("parquet_sha256"), capture=True)
    frame = pl.read_parquet(io.BytesIO(body))
    required = {"release_url", "metric", "period", "published_on", "html_path", "html_sha256"}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError("resume archive lacks required release columns or rows")
    actual_releases = frame["release_url"].n_unique()
    if (actual_releases != prior.get("saved_releases")
            or actual_releases != prior.get("registered_releases")
            or frame.select("release_url", "metric").unique().height != frame.height):
        raise ValueError("resume archive release count or identity differs from receipt")
    receipts = prior.get("listing_receipts")
    if not isinstance(receipts, list) or not receipts or len(receipts) > 1000:
        raise ValueError("recent-page refresh lacks prior full listing receipts")
    pages: set[int] = set()
    for receipt in receipts:
        if not isinstance(receipt, dict):
            raise ValueError("invalid resume listing receipt")
        page = receipt.get("page")
        if (type(page) is not int or not 1 <= page <= 1000 or page in pages
                or receipt.get("url") != LIST_URL.format(page=page)):
            raise ValueError("resume listing page identity differs from receipt")
        pages.add(page)
        listing_body = verify(raw_path(receipt.get("path"), "list"), receipt.get("sha256"),
                              capture=True)
        # A pre-fix completed receipt may pin a wrong/truncated page perfectly.
        # Validate those exact verified bytes, never reopen the path. Recent
        # scans mix generations, so only each page's own totals are authoritative.
        listing = _parse_listing_page(listing_body)
        _validate_listing_page(listing, page)
    rows = frame.to_dicts()
    for row in rows:
        verify(raw_path(row.get("html_path"), "detail"), row.get("html_sha256"))
    return rows


def _collect_one(listed: dict[str, str], root: Path, limiter: SharedRateLimiter,
                 *, refresh: bool) -> list[dict[str, object]]:
    release_id = Path(urlparse(listed["release_url"]).path).stem
    directory = root / "raw" / OUTPUT_NAME / "detail" / release_id
    cached_body = None if refresh else _cached(directory, "article")
    body = cached_body
    if body is None:
        body = _fetch(listed["release_url"], limiter)
    try:
        values, warning = parse_detail(body, listed)
    except ValueError:
        if refresh or cached_body is None:
            raise
        body = _fetch(listed["release_url"], limiter)
        values, warning = parse_detail(body, listed)
    digest, path = _save_raw(directory, "article", body)
    observed = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    return [
        {**listed, "release_id": release_id, "metric": metric,
         "value_pct": value, "value_evidence": "original_press_release_text",
         "published_time_precision": "official_date_only", "parse_warning": None,
         "html_sha256": digest, "html_path": path, "observed_at_utc": observed}
        for metric, value in values.items()
    ] or [{**listed, "release_id": release_id, "metric": None,
           "value_pct": None, "value_evidence": "raw_release_only",
           "published_time_precision": "official_date_only", "parse_warning": warning,
           "html_sha256": digest, "html_path": path, "observed_at_utc": observed}]


def collect(root: Path, *, workers: int = 8, request_interval: float = 0.1,
            refresh_recent: int = 3, recent_pages: int = 0,
            cached_list_pages: bool = False) -> dict[str, object]:
    if not 1 <= workers <= 32 or not 0.1 <= request_interval <= 60 or refresh_recent < 0 or recent_pages < 0:
        raise ValueError("invalid worker, request interval, or refresh setting")
    if recent_pages and cached_list_pages:
        raise ValueError("recent-page refresh cannot use cached index pages")
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    prior_rows: list[dict[str, object]] = []
    prior_receipts: list[dict[str, object]] = []
    prior: dict[str, object] = {}
    if recent_pages:
        prior = read_release_resume_state(root, OUTPUT_NAME)
        if prior is None:
            raise ValueError("recent-page refresh requires a complete prior full archive")
        _write_state(root, {"dataset": OUTPUT_NAME, "status": "running", "phase": "verifying_resume",
                            "started_at_utc": started_at})
        prior_rows = _verified_resume_rows(root, prior)
        prior_receipts = prior["listing_receipts"]
    resume_verification_seconds = round(time.monotonic() - started, 6)
    limiter = SharedRateLimiter(request_interval, name="cbc-release-archive")
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running", "phase": "discovering",
                        "started_at_utc": started_at, "completed_releases": 0,
                        "total_releases": None, "estimated_seconds_remaining": None})
    discovery_started = time.monotonic()
    reference_listing: ListingPage | None = None
    def fetch_page(page: int):
        url = LIST_URL.format(page=page)
        body = (_cached(root / "raw" / OUTPUT_NAME / "list", f"page-{page:04d}", listing=True)
                if cached_list_pages else _fetch(url, limiter))
        if body is None:
            raise FileNotFoundError(f"missing cached CBC listing page: {page}")
        listing = _parse_listing_page(body)
        _validate_listing_page(listing, page, expected=reference_listing)
        digest, path = _save_raw(root / "raw" / OUTPUT_NAME / "list",
                                 f"page-{page:04d}", body)
        return page, digest, path, listing

    first = fetch_page(1)
    reference_listing = first[-1]
    total_pages = first[-1].total_pages
    pages = [first]
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running", "phase": "discovering",
                        "started_at_utc": started_at, "completed_pages": 1,
                        "total_pages": total_pages, "completed_releases": 0,
                        "total_releases": None, "estimated_seconds_remaining": None})
    pages_to_fetch = min(total_pages, recent_pages) if recent_pages else total_pages
    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            futures = [pool.submit(fetch_page, page) for page in range(2, pages_to_fetch + 1)]
            for future in as_completed(futures):
                pages.append(future.result())
                if len(pages) % 20 == 0 or len(pages) == pages_to_fetch:
                    elapsed = max(time.monotonic() - started, 0.001)
                    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running",
                                        "phase": "discovering", "started_at_utc": started_at,
                                        "completed_pages": len(pages), "total_pages": pages_to_fetch,
                                        "completed_releases": 0, "total_releases": None,
                                        "estimated_seconds_remaining": round(
                                            (pages_to_fetch - len(pages)) * elapsed / len(pages)
                                        )})
        except Exception:
            # Includes progress persistence errors, not only failed requests.
            # Already running workers still finish before leaving this pool.
            pool.shutdown(wait=False, cancel_futures=True)
            raise
    listed: dict[str, dict[str, str]] = {}
    receipts: list[dict[str, object]] = []
    for page, digest, path, listing in sorted(pages):
        receipts.append({"page": page, "url": LIST_URL.format(page=page),
                         "sha256": digest, "path": path, "release_rows": len(listing.rows),
                         "raw_rows": listing.raw_rows, "total_rows": listing.total_rows,
                         "total_pages": listing.total_pages, "page_size": listing.page_size})
        for row in listing.rows:
            previous = listed.get(row["release_url"])
            if previous is not None and previous != row:
                raise ValueError(f"CBC money listing conflict: {row['release_url']}")
            listed[row["release_url"]] = row
    discovery_seconds = round(time.monotonic() - discovery_started, 6)
    if recent_pages:
        # Old originals remain immutable evidence even when current pagination
        # shifts.  The full weekly scan detects non-recent index additions.
        fresh_receipts = {int(receipt["page"]): receipt for receipt in receipts}
        receipts = [receipt for receipt in prior_receipts
                    if int(receipt["page"]) not in fresh_receipts] + list(fresh_receipts.values())
        receipts.sort(key=lambda receipt: int(receipt["page"]))
        prior_urls = {str(row["release_url"]) for row in prior_rows}
        newest_from_index = (
            {row["release_url"] for row in sorted(
                listed.values(), key=lambda item: (item["published_on"], item["release_url"])
            )[-refresh_recent:]} if refresh_recent else set()
        )
        ordered = sorted((row for row in listed.values()
                          if row["release_url"] not in prior_urls or row["release_url"] in newest_from_index),
                         key=lambda item: (item["published_on"], item["release_url"]))
    else:
        ordered = sorted(listed.values(), key=lambda item: (item["published_on"], item["release_url"]))
    newest = {row["release_url"] for row in ordered[-refresh_recent:]} if refresh_recent else set()
    rows: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running", "phase": "downloading",
                        "started_at_utc": started_at, "completed_releases": 0,
                        "total_releases": len(ordered), "estimated_seconds_remaining": None})
    last_progress = time.monotonic()
    completed = 0
    detail_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_collect_one, item, root, limiter,
                               refresh=item["release_url"] in newest): item for item in ordered}
        for future in as_completed(futures):
            item = futures[future]
            try:
                rows.extend(future.result())
            except Exception as exc:
                if isinstance(exc, SourceAccessBlocked):
                    pool.shutdown(wait=False, cancel_futures=True)
                    raise
                failures.append({"release_url": item["release_url"],
                                 "error": f"{type(exc).__name__}: {exc}"})
            completed += 1
            now = time.monotonic()
            if now - last_progress >= 5 or completed == len(ordered):
                _write_state(root, {"dataset": OUTPUT_NAME, "status": "running",
                                    "phase": "downloading", "started_at_utc": started_at,
                                    "completed_releases": completed,
                                    "total_releases": len(ordered),
                                    "failed_releases": len(failures),
                                    "estimated_seconds_remaining": round(
                                        (len(ordered) - completed) * (now - started) / completed
                                    ) if completed else None})
                last_progress = now
    detail_seconds = round(time.monotonic() - detail_started, 6)
    if recent_pages:
        refreshed_urls = {str(row["release_url"]) for row in rows}
        rows.extend(row for row in prior_rows if str(row["release_url"]) not in refreshed_urls)
    registered_urls = {str(row["release_url"]) for row in prior_rows} | set(listed)
    periods = {str(row["period"]) for row in rows if row["metric"] == "m1b_yoy_pct"}
    missing_periods: list[str] = []
    if periods:
        year, month = map(int, min(periods).split("-"))
        through = max(periods)
        while f"{year:04d}-{month:02d}" <= through:
            period = f"{year:04d}-{month:02d}"
            if period not in periods:
                missing_periods.append(period)
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    rows.sort(key=lambda row: (str(row["published_on"]), str(row["release_url"]),
                               str(row["metric"])))
    # Archive completeness is about discovered article bytes. Value-history
    # completeness is a separate claim: an old release may be raw-only or a
    # monthly issue may be absent from the official index altogether.
    complete = (not cached_list_pages and not failures
                and registered_urls == {str(row["release_url"]) for row in rows})
    summary: dict[str, object] = {
        "dataset": OUTPUT_NAME, "status": "complete" if complete else "degraded",
        "complete": complete, "value_history_complete": not missing_periods,
        "registered_releases": len(registered_urls),
        "saved_releases": len({str(row["release_url"]) for row in rows}),
        "value_releases": len(periods), "earliest_period": min(periods) if periods else None,
        "latest_period": max(periods) if periods else None,
        "missing_periods": missing_periods, "failures": failures,
        "listing_receipts": receipts, "started_at_utc": started_at,
        "scan_scope": ("recent_pages" if recent_pages else
                       "cached_full_index" if cached_list_pages else "full_index"),
        "scanned_pages": pages_to_fetch, "index_total_pages": total_pages,
        "last_full_index_scan_at_utc": (
            prior.get("last_full_index_scan_at_utc") or (
                prior.get("generated_at_utc") if prior.get("scan_scope") == "full_index" else None
            )
            if recent_pages else None if cached_list_pages else
            datetime.now(timezone.utc).isoformat(timespec="microseconds")
        ),
        "stage_seconds": {"resume_verification": resume_verification_seconds,
                          "listing_discovery": discovery_seconds,
                          "detail_collection": detail_seconds},
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    if rows and not failures:
        write_started = time.monotonic()
        path = root / f"{OUTPUT_NAME}.parquet"
        digest, changed = write_release_rows_if_changed(path, rows,
                                                         identity_columns=("release_url", "metric"),
                                                         allow_placeholder_upgrade=True)
        summary.update({"parquet_path": str(path), "parquet_sha256": digest,
                        "parquet_changed": changed})
        summary["stage_seconds"]["parquet_proof_write"] = round(
            time.monotonic() - write_started, 6
        )
    summary["elapsed_seconds"] = round(time.monotonic() - started, 6)
    summary["elapsed_scope"] = "through_parquet_proof_before_final_state_write"
    _write_state(root, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-interval", type=float, default=0.1)
    parser.add_argument("--refresh-recent", type=int, default=3)
    parser.add_argument("--recent-pages", type=int, default=0,
                        help="Refresh only newest index pages, retaining a completed full archive.")
    parser.add_argument("--cached-list-pages", action="store_true",
                        help="Reparse locally saved full index after a failed write; does not prove current listing freshness.")
    args = parser.parse_args()
    root = args.output_dir.resolve()
    with _writer_lock(root):
        try:
            summary = collect(root, workers=args.workers,
                              request_interval=args.request_interval,
                              refresh_recent=args.refresh_recent,
                              recent_pages=args.recent_pages,
                              cached_list_pages=args.cached_list_pages)
        except Exception as exc:
            _write_state(root, {"dataset": OUTPUT_NAME, "status": "degraded",
                                "complete": False, "error": f"{type(exc).__name__}: {exc}",
                                "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds")})
            raise
    print(json.dumps({key: value for key, value in summary.items()
                      if key not in {"listing_receipts", "failures"}}, ensure_ascii=False))
    if not summary["complete"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
