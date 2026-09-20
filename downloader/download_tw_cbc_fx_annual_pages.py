"""Collect CBC's freely accessible annual USD/TWD daily-close tables.

The annual tables extend the currently registered open-data CSV back to 2000.
They are present-day historical tables, not proof of immutable day-one values.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import polars as pl

try:
    from downloader.common import SharedRateLimiter
    from downloader.download_tw_cbc_fx_release_archive import _fetch, _save_raw
    from downloader.tw_public_source_lock import source_update_lock
except ImportError:  # direct invocation from downloader/
    from common import SharedRateLimiter
    from download_tw_cbc_fx_release_archive import _fetch, _save_raw
    from tw_public_source_lock import source_update_lock


NAME = "cbc_usdtwd_annual_pages"
BASE = "https://www.cbc.gov.tw"
INDEX = BASE + "/tw/lp-2151-1.html"


def parse_index(body: bytes) -> tuple[dict[int, str], int]:
    soup = BeautifulSoup(body, "html.parser")
    text = soup.get_text(" ", strip=True)
    match = re.search(r"共\s*\d+\s*筆資料，第\s*\d+\s*/\s*(\d+)\s*頁", text)
    if match is None:
        raise ValueError("CBC FX annual index lacks a page-count receipt")
    links: dict[int, str] = {}
    for link in soup.select('a[href^="/tw/cp-2151-"]'):
        title = link.get_text(" ", strip=True)
        year_match = re.match(r"^(\d{4})年 新臺幣對美元銀行間成交之收盤匯率", title)
        if year_match is None:
            continue
        year = int(year_match.group(1))
        url = urljoin(BASE, link["href"])
        if urlparse(url).hostname != "www.cbc.gov.tw":
            raise ValueError("CBC FX annual link escapes official host")
        if year in links and links[year] != url:
            raise ValueError(f"two different CBC FX annual links for {year}")
        links[year] = url
    return links, int(match.group(1))


def parse_annual(body: bytes, year: int) -> list[dict]:
    soup = BeautifulSoup(body, "html.parser")
    heading = soup.select_one("h2.title")
    table = soup.select_one("section.cp table")
    if heading is None or not heading.get_text(" ", strip=True).startswith(f"{year}年 新臺幣對美元") or table is None:
        raise ValueError(f"CBC FX annual page missing title/table for {year}")
    headers = [cell.get_text(" ", strip=True) for cell in table.select("thead th")]
    if headers != ["Date", "NTD/USD"]:
        raise ValueError(f"CBC FX annual table headings changed for {year}: {headers}")
    rows = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) != 2:
            continue
        day = datetime.strptime(cells[0].get_text(strip=True), "%Y/%m/%d").date()
        rate = float(cells[1].get_text(strip=True))
        if day.year != year or not 10 < rate < 100:
            raise ValueError(f"CBC FX annual date/rate invalid in {year}: {day} {rate}")
        rows.append({"subject_date": day.isoformat(), "ntd_per_usd": rate})
    if len(rows) < 100:
        raise ValueError(f"CBC FX annual page unexpectedly short for {year}: {len(rows)}")
    if len({row["subject_date"] for row in rows}) != len(rows):
        raise ValueError(f"CBC FX annual page duplicates a date in {year}")
    return rows


def collect(root: Path, *, request_interval: float = 0.5) -> dict:
    if request_interval <= 0:
        raise ValueError("request interval must be positive")
    limiter = SharedRateLimiter(request_interval, name="cbc_fx_annual_pages")
    index_dir = root / "raw" / NAME / "index"
    detail_dir = root / "raw" / NAME / "annual"
    first = _fetch(INDEX, limiter)
    listings: dict[int, str] = {}
    first_links, pages = parse_index(first)
    for page in range(1, pages + 1):
        url = INDEX if page == 1 else f"{BASE}/tw/lp-2151-1-{page}-20.html"
        body = first if page == 1 else _fetch(url, limiter)
        found, observed_pages = parse_index(body)
        if observed_pages != pages:
            raise ValueError("CBC FX annual index changed during scan")
        _save_raw(index_dir, f"page-{page:02d}", body)
        for year, link in found.items():
            if year in listings and listings[year] != link:
                raise ValueError(f"CBC FX annual index conflicts for {year}")
            listings[year] = link
    if not listings:
        raise ValueError("CBC FX annual index has no year links")
    output = root / "supplemental" / f"{NAME}.parquet"
    old = pl.read_parquet(output).to_dicts() if output.is_file() else []
    prior = {str(row["subject_date"]): row for row in old}
    rows: list[dict] = []
    for year, url in sorted(listings.items()):
        body = _fetch(url, limiter)
        parsed = parse_annual(body, year)
        digest, raw_path = _save_raw(detail_dir, str(year), body)
        observed = datetime.now(timezone.utc).isoformat()
        for row in parsed:
            item = {**row, "page_url": url, "page_sha256": digest,
                    "raw_path": raw_path, "observed_at_utc": observed}
            previous = prior.get(row["subject_date"])
            rows.append(previous if previous is not None and previous["ntd_per_usd"] == row["ntd_per_usd"] else item)
    rows.sort(key=lambda row: row["subject_date"])
    if len({row["subject_date"] for row in rows}) != len(rows):
        raise ValueError("CBC FX annual index has overlapping/duplicate daily values")
    output.parent.mkdir(parents=True, exist_ok=True)
    if rows != old:
        temporary = output.with_suffix(".parquet.tmp")
        pl.DataFrame(rows).write_parquet(temporary, compression="zstd")
        os.replace(temporary, output)
    state = {"dataset": NAME, "status": "complete", "years": sorted(listings),
             "index_pages": pages, "distinct_dates": len(rows),
             "first_subject_date": rows[0]["subject_date"],
             "last_subject_date": rows[-1]["subject_date"],
             "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    state_path = root / "state" / f"{NAME}.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_state = state_path.with_suffix(".json.tmp")
    temporary_state.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary_state, state_path)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--source-update-lock-held", action="store_true")
    args = parser.parse_args()
    root = args.output_dir.resolve()
    lock_path = root / "state/locks" / f"{NAME}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with source_update_lock(root, already_held=args.source_update_lock_held):
        with lock_path.open("a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("CBC FX annual collector already active") from exc
            summary = collect(root, request_interval=args.request_interval)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
