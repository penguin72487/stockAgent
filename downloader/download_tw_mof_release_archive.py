"""Archive indexed MOF trade and tax original releases with immutable receipts.

This is an archive of the official documents currently linked by the MOF
release index. A historical subject month or old publication date does not
prove that today's downloaded bytes are the first published vintage.
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
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
import polars as pl
from pypdf import PdfReader
from pypdf.errors import PdfReadError
import requests

try:
    from downloader.common import SharedRateLimiter
    from downloader.download_tw_mof_trade_release_values import _fetch
    from downloader.release_archive_io import write_release_rows_if_changed
    from downloader.tw_public_source_lock import source_update_lock
except ImportError:  # direct invocation from downloader/
    from common import SharedRateLimiter
    from download_tw_mof_trade_release_values import _fetch
    from release_archive_io import write_release_rows_if_changed
    from tw_public_source_lock import source_update_lock


NAME = "mof_original_release_archive"
SERIES = frozenset({"trade", "tax"})
ALLOWED_HOSTS = frozenset({"www.mof.gov.tw", "service.mof.gov.tw"})
MAX_DETAIL_BYTES = 2_000_000
MAX_PDF_BYTES = 32_000_000


def _official_url(url: str, *, pdf: bool = False) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in ALLOWED_HOSTS:
        raise ValueError(f"out-of-scope MOF URL: {url}")
    if parsed.username or parsed.password or parsed.port:
        raise ValueError(f"MOF URL has unexpected authority: {url}")
    if pdf and not (parsed.path.casefold().endswith(".pdf") or
                    (parsed.hostname == "www.mof.gov.tw" and
                     re.fullmatch(r"/download/[A-Za-z0-9_-]+", parsed.path))):
        raise ValueError(f"MOF attachment is not a PDF URL: {url}")
    return urlunparse(parsed._replace(scheme="https", fragment=""))


def select_original_pdf_url(detail: bytes) -> str:
    """Choose the full press release when linked, otherwise its body PDF."""
    soup = BeautifulSoup(detail, "html.parser", from_encoding="utf-8")
    candidates: list[tuple[int, str]] = []
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "")
        if not (urlparse(href).path.casefold().endswith(".pdf") or
                re.fullmatch(r"/download/[A-Za-z0-9_-]+", urlparse(href).path)):
            continue
        label = f"{anchor.get('title') or ''} {anchor.get_text(' ', strip=True)}"
        if "本文及附表" in label:
            priority = 3
        elif "新聞稿本文" in label or "中文新聞稿" in label:
            priority = 2
        else:
            continue
        candidates.append((priority, _official_url(urljoin("https://www.mof.gov.tw", href), pdf=True)))
    if not candidates:
        raise ValueError("MOF detail has no labelled original press PDF")
    best = max(score for score, _ in candidates)
    urls = list(dict.fromkeys(url for score, url in candidates if score == best))
    if len(urls) != 1:
        raise ValueError(f"MOF detail has ambiguous original PDFs: {urls}")
    return urls[0]


def select_body_pdf_url(detail: bytes) -> str | None:
    soup = BeautifulSoup(detail, "html.parser", from_encoding="utf-8")
    candidates = []
    for anchor in soup.select("a[href]"):
        label = f"{anchor.get('title') or ''} {anchor.get_text(' ', strip=True)}"
        if "新聞稿本文" not in label or "附表" in label:
            continue
        href = str(anchor.get("href") or "")
        if not (urlparse(href).path.casefold().endswith(".pdf") or
                re.fullmatch(r"/download/[A-Za-z0-9_-]+", urlparse(href).path)):
            continue
        candidates.append(_official_url(urljoin("https://www.mof.gov.tw", href), pdf=True))
    urls = list(dict.fromkeys(candidates))
    if len(urls) > 1:
        raise ValueError(f"MOF detail has ambiguous body PDFs: {urls}")
    return urls[0] if urls else None


def _save_bytes(path: Path, body: bytes) -> str:
    digest = hashlib.sha256(body).hexdigest()
    if path.is_file():
        with path.open("rb") as handle:
            if hashlib.file_digest(handle, "sha256").hexdigest() != digest:
                raise RuntimeError(f"immutable MOF archive checksum mismatch: {path}")
        return digest
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(body)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return digest


def _pdf_page_count(body: bytes, *, period: str, series: str) -> tuple[int, bool]:
    if not body.startswith(b"%PDF-"):
        raise ValueError("MOF original response is not a PDF")
    reader = PdfReader(io.BytesIO(body), strict=False)
    if reader.is_encrypted or not reader.pages:
        raise ValueError("MOF original PDF is encrypted or empty")
    text = re.sub(r"\s+", "", "".join(
        page.extract_text() or "" for page in reader.pages[:2]
    ))
    roc_year, month = int(period[:4]) - 1911, int(period[-2:])
    subject = "海關進出口貿易" if series == "trade" else "全國賦稅收入"
    heading = re.search(rf"{roc_year}年0?{month}月.{{0,20}}{subject}", text)
    return len(reader.pages), heading is not None


def _verified_previous(row: dict[str, object]) -> bool:
    try:
        pdf = Path(str(row["pdf_raw_path"]))
        if not pdf.is_file() or pdf.stat().st_size != int(row["pdf_bytes"]):
            return False
        with pdf.open("rb") as handle:
            if hashlib.file_digest(handle, "sha256").hexdigest() != row["pdf_sha256"]:
                return False
        detail = row.get("detail_raw_path")
        if detail:
            path = Path(str(detail))
            if not path.is_file():
                return False
            with path.open("rb") as handle:
                if hashlib.file_digest(handle, "sha256").hexdigest() != row["detail_sha256"]:
                    return False
        body = row.get("body_raw_path")
        if body:
            body_file = Path(str(body))
            if not body_file.is_file() or body_file.stat().st_size != int(row["body_bytes"]):
                return False
            with body_file.open("rb") as handle:
                if hashlib.file_digest(handle, "sha256").hexdigest() != row["body_sha256"]:
                    return False
        return True
    except (OSError, TypeError, ValueError, KeyError):
        return False


def _capture(root: Path, release: dict[str, object], limiter: SharedRateLimiter) -> dict[str, object]:
    series, period = str(release["series"]), str(release["period"])
    url = _official_url(str(release["release_url"]))
    detail_digest: str | None = None
    detail_path: str | None = None
    detail: bytes | None = None
    if (urlparse(url).path.startswith("/download/") or
            (urlparse(url).hostname == "service.mof.gov.tw" and
             urlparse(url).path.casefold().endswith(".pdf"))):
        pdf_url = url
    else:
        if not urlparse(url).path.startswith("/singlehtml/"):
            raise ValueError(f"unexpected MOF original URL: {url}")
        detail = _fetch(url, limiter, maximum=MAX_DETAIL_BYTES)
        detail_digest = hashlib.sha256(detail).hexdigest()
        detail_file = root / "raw" / NAME / series / period / f"detail-{detail_digest}.html"
        _save_bytes(detail_file, detail)
        detail_path = str(detail_file)
        pdf_url = select_original_pdf_url(detail)
    pdf = _fetch(pdf_url, limiter, maximum=MAX_PDF_BYTES)
    page_count, text_period_verified = _pdf_page_count(pdf, period=period, series=series)
    pdf_digest = hashlib.sha256(pdf).hexdigest()
    pdf_file = root / "raw" / NAME / series / period / f"original-{pdf_digest}.pdf"
    _save_bytes(pdf_file, pdf)
    body_url = None
    body_path = None
    body_digest = None
    body_bytes = None
    body_pages = None
    if not text_period_verified and detail is not None:
        body_url = select_body_pdf_url(detail)
        if body_url and body_url != pdf_url:
            body = _fetch(body_url, limiter, maximum=MAX_PDF_BYTES)
            body_pages, body_verified = _pdf_page_count(body, period=period, series=series)
            body_digest = hashlib.sha256(body).hexdigest()
            body_file = root / "raw" / NAME / series / period / f"body-{body_digest}.pdf"
            _save_bytes(body_file, body)
            body_path = str(body_file)
            body_bytes = len(body)
            text_period_verified = body_verified
    return {
        "series": series, "period": period, "published_on": str(release["published_on"]),
        "release_url": url, "pdf_url": pdf_url,
        "detail_raw_path": detail_path, "detail_sha256": detail_digest,
        "pdf_raw_path": str(pdf_file), "pdf_sha256": pdf_digest,
        "pdf_bytes": len(pdf), "pdf_pages": page_count,
        "text_period_verified": text_period_verified,
        "subject_rule_version": 2,
        "subject_body_checked": True,
        "body_pdf_url": body_url, "body_raw_path": body_path,
        "body_sha256": body_digest, "body_bytes": body_bytes,
        "body_pages": body_pages,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _period_gaps(periods: list[str]) -> list[str]:
    if not periods:
        return []
    first, last = (int(value[:4]) * 12 + int(value[-2:])
                   for value in (min(periods), max(periods)))
    observed = set(periods)
    return [f"{(ordinal - 1) // 12:04d}-{(ordinal - 1) % 12 + 1:02d}"
            for ordinal in range(first, last + 1)
            if f"{(ordinal - 1) // 12:04d}-{(ordinal - 1) % 12 + 1:02d}" not in observed]


def collect(root: Path, *, request_interval: float = 0.5,
            recent_periods: int = 3, full_recheck: bool = False) -> dict[str, object]:
    if request_interval <= 0 or recent_periods < 0:
        raise ValueError("request interval must be positive and recent-periods nonnegative")
    index_path = root / "supplemental/mof_macro_release_dates.parquet"
    if not index_path.is_file():
        raise FileNotFoundError(f"MOF release index is missing: {index_path}")
    index = pl.read_parquet(index_path).to_dicts()
    source: dict[tuple[str, str], dict[str, object]] = {}
    for release in index:
        series, period = release["series"], release["period"]
        if series not in SERIES or not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", period):
            raise ValueError(f"invalid MOF index identity: {series}/{period}")
        key = series, period
        if key in source:
            raise ValueError(f"duplicate MOF index period: {key}")
        source[key] = release
    if not source:
        raise ValueError("MOF index has no trade or tax releases")
    output = root / "supplemental" / f"{NAME}.parquet"
    previous = pl.read_parquet(output).to_dicts() if output.is_file() else []
    by_key = {(row["series"], row["period"]): row for row in previous}
    index_regressions = [f"{series}/{period}" for series, period in sorted(by_key)
                         if (series, period) not in source]
    newest = {series: set(sorted((period for s, period in source if s == series),
                                 reverse=True)[:recent_periods]) for series in SERIES}
    limiter = SharedRateLimiter(request_interval, name=NAME)
    failures: list[dict[str, str]] = []
    revalidation_failures: list[dict[str, str]] = []
    refreshed = 0
    locally_reverified = 0
    for key, release in sorted(source.items()):
        old = by_key.get(key)
        if (old is not None and not full_recheck and key[1] not in newest[key[0]]
                and old.get("release_url") == _official_url(str(release["release_url"]))
                and old.get("published_on") == release["published_on"]
                and (old.get("text_period_verified") or old.get("subject_body_checked"))
                and _verified_previous(old)):
            if old.get("subject_rule_version") != 2:
                try:
                    pages, verified = _pdf_page_count(
                        Path(str(old["pdf_raw_path"])).read_bytes(),
                        period=key[1], series=key[0],
                    )
                    by_key[key] = {**old, "pdf_pages": pages,
                                   "text_period_verified": verified,
                                   "subject_rule_version": 2}
                    locally_reverified += 1
                except (OSError, ValueError, PdfReadError, IndexError) as exc:
                    revalidation_failures.append({
                        "series": key[0], "period": key[1],
                        "error": f"{type(exc).__name__}: {exc}",
                    })
            continue
        try:
            captured = _capture(root, release, limiter)
            by_key[key] = captured
            refreshed += 1
        except (OSError, RuntimeError, ValueError, PdfReadError,
                IndexError, requests.RequestException) as exc:
            failures.append({"series": key[0], "period": key[1],
                             "error": f"{type(exc).__name__}: {exc}"})
    rows = [by_key[key] for key in sorted(by_key)]
    if rows:
        parquet_sha256, changed = write_release_rows_if_changed(
            output, rows, identity_columns=("series", "period")
        )
    else:
        parquet_sha256, changed = None, False
    gaps = {series: _period_gaps([period for s, period in source if s == series])
            for series in sorted(SERIES)}
    uncaptured = [f"{series}/{period}" for series, period in sorted(source)
                  if (series, period) not in by_key or not _verified_previous(by_key[series, period])]
    subject_unverified = [f"{row['series']}/{row['period']}" for row in rows
                          if not row.get("text_period_verified")]
    state = {
        "dataset": NAME,
        "status": "complete" if not failures and not revalidation_failures
                  and not uncaptured and not index_regressions
                  else "degraded",
        "indexed_releases": len(source), "archived_releases": len(source) - len(uncaptured),
        "refreshed_releases": refreshed, "uncaptured_releases": uncaptured,
        "locally_reverified_releases": locally_reverified,
        "subject_unverified_releases": subject_unverified,
        "index_regressions": index_regressions,
        "index_period_gaps": gaps, "continuous_index_history": not any(gaps.values()),
        "earliest_period_by_series": {series: min(period for s, period in source if s == series)
                                      for series in sorted(SERIES)},
        "latest_period_by_series": {series: max(period for s, period in source if s == series)
                                    for series in sorted(SERIES)},
        "failures": failures, "parquet_path": str(output),
        "revalidation_failures": revalidation_failures,
        "parquet_sha256": parquet_sha256, "parquet_changed": changed,
        "vintage_rule": "observed_bytes_only_first_publication_version_unverified",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    state_path = root / "state" / f"{NAME}.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, state_path)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--request-interval", type=float, default=0.5)
    parser.add_argument("--recent-periods", type=int, default=3)
    parser.add_argument("--full-recheck", action="store_true")
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
                raise RuntimeError("MOF original release archive is already active") from exc
            state = collect(root, request_interval=args.request_interval,
                            recent_periods=args.recent_periods, full_recheck=args.full_recheck)
    print(json.dumps(state, ensure_ascii=False), flush=True)
    return 0 if state["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
