"""Collect CBC's dated overnight-rate pages, including the stale-open-data tail.

These pages establish reported values and observation dates, but not the exact
historical first-publication time or an immutable first-published value vintage.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re

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


NAME = "cbc_overnight_official_pages"
BASE = "https://www.cbc.gov.tw/tw/lp-641-1"
INDEX = BASE + ".html"


def parse_page(body: bytes) -> tuple[list[dict], int, int]:
    soup = BeautifulSoup(body, "html.parser")
    table = soup.select_one("section.lp table.rwd-table")
    if table is None:
        raise ValueError("CBC overnight page lacks its expected data table")
    headers = [cell.get_text(" ", strip=True) for cell in table.select("tr:first-child th")]
    if headers != ["標題(顯示資料日期)", "利率"]:
        raise ValueError(f"CBC overnight table headings changed: {headers}")
    rows: list[dict] = []
    for tr in table.select("tr"):
        cells = tr.find_all("td")
        if len(cells) != 2:
            continue
        subject = datetime.strptime(cells[0].get_text(strip=True), "%Y/%m/%d").date()
        rate = float(cells[1].get_text(strip=True))
        if not 0 <= rate <= 100:
            raise ValueError(f"implausible CBC overnight percentage: {rate}")
        rows.append({"subject_date": subject.isoformat(), "rate_pct": rate})
    if not rows:
        raise ValueError("CBC overnight page contains no observations")
    match = re.search(r"共\s*(\d+)\s*筆資料，第\s*\d+\s*/\s*(\d+)\s*頁", soup.get_text(" ", strip=True))
    if match is None:
        raise ValueError("CBC overnight page lacks a count/page receipt")
    return rows, int(match.group(2)), int(match.group(1))


def collect(root: Path, *, request_interval: float = 0.5, recent_pages: int = 10) -> dict:
    if request_interval <= 0 or recent_pages < 0:
        raise ValueError("request interval must be positive and recent-pages nonnegative")
    output = root / "supplemental" / f"{NAME}.parquet"
    old = pl.read_parquet(output).to_dicts() if output.is_file() else []
    state_path = root / "state" / f"{NAME}.json"
    try:
        prior_state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        prior_state = {}
    full_history_scanned = bool(prior_state.get("full_history_scanned")) or prior_state.get("status") == "complete"
    limiter = SharedRateLimiter(request_interval, name="cbc_official_daily_pages")
    raw_dir = root / "raw" / NAME
    first_body = _fetch(INDEX, limiter)
    first_rows, page_count, advertised_total = parse_page(first_body)
    pages_to_read = page_count if not old or not full_history_scanned or recent_pages == 0 else min(page_count, recent_pages)
    read_rows: list[dict] = []
    for page in range(1, pages_to_read + 1):
        url = INDEX if page == 1 else f"{BASE}-{page}-20.html"
        body = first_body if page == 1 else _fetch(url, limiter)
        parsed, observed_page_count, observed_total = (first_rows, page_count, advertised_total) if page == 1 else parse_page(body)
        if observed_page_count != page_count or observed_total != advertised_total:
            raise ValueError("CBC overnight pagination changed during scan")
        digest, raw_path = _save_raw(raw_dir, f"page-{page:03d}", body)
        read_rows.extend({**row, "page_url": url, "page_sha256": digest,
                          "raw_path": raw_path,
                          "observed_at_utc": datetime.now(timezone.utc).isoformat()}
                         for row in parsed)
    by_date: dict[str, list[dict]] = {}
    fresh_by_date: dict[str, list[dict]] = {}
    old_by_date = {str(row["subject_date"]): row for row in old}
    for row in old:
        by_date.setdefault(str(row["subject_date"]), []).append(row)
    for row in read_rows:
        by_date.setdefault(row["subject_date"], []).append(row)
        fresh_by_date.setdefault(row["subject_date"], []).append(row)
    rows = []
    for subject, candidates in sorted(by_date.items()):
        fresh = fresh_by_date.get(subject, [])
        selected = fresh if fresh else candidates
        values = sorted({float(row["rate_pct"]) for row in selected if row.get("rate_pct") is not None})
        first = selected[0]
        original = old_by_date.get(subject)
        if original is not None and len(values) == 1 and original.get("status") == "ok" and original.get("rate_pct") == values[0]:
            # Re-polling an unchanged page is not a new value vintage.
            first = original
        rows.append({"subject_date": subject,
                     "rate_pct": values[0] if len(values) == 1 else None,
                     "candidate_rates_pct": json.dumps(values),
                     "status": "ok" if len(values) == 1 else "ambiguous",
                     "page_url": first["page_url"], "page_sha256": first["page_sha256"],
                     "raw_path": first["raw_path"],
                     "observed_at_utc": first["observed_at_utc"]})
    # A full index must reconcile page observations before promotion. The site
    # can repeat a date, so compare distinct dates plus explicit duplicates.
    if pages_to_read == page_count and len(read_rows) != advertised_total:
        raise ValueError(
            f"CBC overnight full scan rows {len(read_rows)} != advertised {advertised_total}"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    if rows != old:
        temporary = output.with_suffix(".parquet.tmp")
        pl.DataFrame(rows).write_parquet(temporary, compression="zstd")
        os.replace(temporary, output)
    full_history_scanned = full_history_scanned or pages_to_read == page_count
    state = {"dataset": NAME, "status": "complete" if full_history_scanned else "partial",
             "full_history_scanned": full_history_scanned,
             "full_index_pages": page_count, "pages_read": pages_to_read,
             "advertised_source_rows": advertised_total,
             "distinct_dates": len(rows), "last_subject_date": rows[-1]["subject_date"],
             "ambiguous_dates": [row["subject_date"] for row in rows if row["status"] == "ambiguous"],
             "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_state = state_path.with_suffix(".json.tmp")
    temporary_state.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary_state, state_path)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--recent-pages", type=int, default=10,
                        help="Zero scans the complete official table; first run always scans all pages.")
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
                raise RuntimeError("CBC overnight page collector is already active") from exc
            summary = collect(root, request_interval=args.request_interval,
                              recent_pages=args.recent_pages)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
