"""Recover MOF trade values missing from the bulk CSV from dated press PDFs.

The PDF's NTD table rounds to whole hundred-million dollars. These values are
kept separate from the exact-thousand-unit customs CSV and never promoted to
strict PIT features at a precision the original release did not publish.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import polars as pl
from pypdf import PdfReader
import requests

try:
    from downloader.common import SharedRateLimiter, retry_delay_seconds
    from downloader.download_tw_cbc_fx_release_archive import _save_raw
    from downloader.tw_public_source_lock import source_update_lock
except ImportError:  # direct invocation from downloader/
    from common import SharedRateLimiter, retry_delay_seconds
    from download_tw_cbc_fx_release_archive import _save_raw
    from tw_public_source_lock import source_update_lock


NAME = "mof_trade_release_values"


def _fetch(url: str, limiter: SharedRateLimiter, *, maximum: int) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in {"www.mof.gov.tw", "service.mof.gov.tw"}:
        raise ValueError(f"out-of-scope MOF release URL: {url}")
    for attempt in range(5):
        limiter.wait()
        try:
            with requests.get(url, timeout=(10, 45), stream=True, allow_redirects=False,
                              headers={"User-Agent": "stockAgent-official-trade-release/1.0"}) as response:
                if response.status_code in {429, 500, 502, 503, 504}:
                    limiter.defer(retry_delay_seconds(
                        attempt, base=1.0, cap=30.0,
                        retry_after=response.headers.get("Retry-After")))
                    continue
                response.raise_for_status()
                if response.is_redirect:
                    raise ValueError(f"unexpected MOF release redirect: {url}")
                if int(response.headers.get("Content-Length") or 0) > maximum:
                    raise ValueError(f"MOF release exceeds byte cap: {url}")
                chunks, total = [], 0
                for chunk in response.iter_content(chunk_size=128_000):
                    total += len(chunk)
                    if total > maximum:
                        raise ValueError(f"MOF release exceeds byte cap: {url}")
                    chunks.append(chunk)
                body = b"".join(chunks)
                if not body:
                    raise ValueError(f"empty MOF release: {url}")
                return body
        except requests.RequestException:
            if attempt == 4:
                raise
            limiter.defer(retry_delay_seconds(attempt, base=1.0, cap=30.0))
    raise RuntimeError(f"MOF release exhausted retries: {url}")


def parse_detail_pdf_url(body: bytes) -> str:
    soup = BeautifulSoup(body, "html.parser")
    for link in soup.select('a[href$=".pdf"]'):
        title = str(link.get("title") or "")
        if "新聞稿本文" not in title:
            continue
        url = str(link["href"])
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname != "service.mof.gov.tw":
            raise ValueError(f"MOF PDF link leaves the official host: {url}")
        return url
    raise ValueError("MOF detail has no press-release body PDF")


def parse_trade_pdf(body: bytes, *, period: str) -> dict[str, float]:
    reader = PdfReader(io.BytesIO(body))
    if not reader.pages:
        raise ValueError("MOF trade PDF has no pages")
    text = reader.pages[0].extract_text() or ""
    return parse_trade_pdf_text(text, period=period)


def parse_trade_pdf_text(text: str, *, period: str) -> dict[str, float]:
    roc_year, month = int(period[:4]) - 1911, int(period[-2:])
    if not re.search(rf"{roc_year}\s*年\s*{month}\s*月海關進出口貿易初步統計", text):
        raise ValueError(f"MOF PDF does not match release period {period}")
    marker = re.search(r"按新臺幣計算\s*[（(]\s*億元\s*[）)]", text)
    if marker is None:
        raise ValueError("MOF trade PDF has no NTD hundred-million table")
    lines = [line.strip() for line in text[marker.end():].splitlines() if line.strip()]
    values: dict[str, float] = {}
    for line in lines[:8]:
        match = re.match(r"^(出口|進口|出超|入超)\s+(-?[\d,]+)(?:\s|$)", line)
        if match is None:
            continue
        label, amount = match.groups()
        key = {"出口": "exports", "進口": "imports", "出超": "balance", "入超": "balance"}[label]
        values[key] = float(amount.replace(",", "")) * 100_000 * (-1 if label == "入超" else 1)
    if set(values) != {"exports", "imports", "balance"}:
        raise ValueError(f"MOF trade PDF lacks a complete NTD table: {period} {values}")
    if abs((values["exports"] - values["imports"]) - values["balance"]) > 100_000:
        raise ValueError(f"MOF rounded trade values are inconsistent: {period} {values}")
    return values


def collect(root: Path, *, request_interval: float = 0.5) -> dict:
    if request_interval <= 0:
        raise ValueError("request interval must be positive")
    releases = root / "supplemental/mof_macro_release_dates.parquet"
    bulk = root / "mof_customs_trade.parquet"
    if not releases.is_file() or not bulk.is_file():
        raise FileNotFoundError("MOF release-date index and customs bulk CSV are required")
    bulk_periods = {
        f"{int(row['年度']) + 1911:04d}-{int(row['月份']):02d}"
        for row in pl.read_parquet(bulk, columns=["年度", "月份"]).to_dicts()
    }
    output = root / "supplemental" / f"{NAME}.parquet"
    old = pl.read_parquet(output).to_dicts() if output.is_file() else []
    by_period = {row["period"]: row for row in old}
    missing = [row for row in pl.read_parquet(releases).to_dicts()
               if row["series"] == "trade" and row["period"] not in bulk_periods]
    limiter = SharedRateLimiter(request_interval, name="mof_trade_release_pdfs")
    failures = []
    for release in sorted(missing, key=lambda row: row["period"]):
        period = release["period"]
        if period in by_period:
            continue
        try:
            detail_url = release["release_url"]
            if urlparse(detail_url).path.startswith("/download/"):
                pdf_url = detail_url
            else:
                detail = _fetch(detail_url, limiter, maximum=2_000_000)
                _save_raw(root / "raw" / NAME / "details", period, detail)
                pdf_url = parse_detail_pdf_url(detail)
            pdf = _fetch(pdf_url, limiter, maximum=8_000_000)
            values = parse_trade_pdf(pdf, period=period)
            digest = hashlib.sha256(pdf).hexdigest()
            raw_path = root / "raw" / NAME / "pdf" / f"{period}-{digest[:16]}.pdf"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            if raw_path.exists():
                if hashlib.sha256(raw_path.read_bytes()).hexdigest() != digest:
                    raise RuntimeError(f"MOF cached PDF checksum mismatch: {raw_path}")
            else:
                temporary = raw_path.with_suffix(".pdf.tmp")
                temporary.write_bytes(pdf)
                os.replace(temporary, raw_path)
            by_period[period] = {"period": period, "published_on": release["published_on"],
                                 **values, "precision_twd_thousand": 100_000,
                                 "release_url": detail_url, "pdf_url": pdf_url,
                                 "pdf_sha256": digest, "raw_path": str(raw_path),
                                 "observed_at_utc": datetime.now(timezone.utc).isoformat()}
        except (ValueError, requests.RequestException) as exc:
            failures.append({"period": period, "error": f"{type(exc).__name__}: {exc}"})
    rows = [by_period[period] for period in sorted(by_period)]
    if rows and rows != old:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(".parquet.tmp")
        pl.DataFrame(rows).write_parquet(temporary, compression="zstd")
        os.replace(temporary, output)
    unresolved = [release["period"] for release in missing if release["period"] not in by_period]
    state = {"dataset": NAME, "status": "complete" if not unresolved else "degraded",
             "bulk_latest_period": max(bulk_periods), "pdf_value_periods": len(rows),
             "missing_bulk_periods": [release["period"] for release in missing],
             "unresolved_periods": unresolved, "failures": failures,
             "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    state_path = root / "state" / f"{NAME}.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, state_path)
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
                raise RuntimeError("MOF trade PDF collector is already active") from exc
            summary = collect(root, request_interval=args.request_interval)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
