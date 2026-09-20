"""Archive dated DGBAS releases, original attachments, values and PDF clocks.

Current-value data.gov tables are not historical vintages. Unknown values
remain null; old periods in current tables are never relabelled as old
releases. The feature builder owns the historical 08:30/16:00 availability
schedule when an original document lacks its own clock.
"""

from __future__ import annotations

import argparse
import atexit
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import tempfile
import threading
import time
import unicodedata
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
import requests
from pypdf import PdfReader

try:
    from downloader.common import SharedRateLimiter, retry_delay_seconds
    from downloader.release_archive_io import write_release_rows_if_changed
except ImportError:  # direct execution from downloader/
    from common import SharedRateLimiter, retry_delay_seconds
    from release_archive_io import write_release_rows_if_changed

try:
    import fcntl
except ImportError:  # pragma: no cover - POSIX is the production runtime
    fcntl = None


BASE = "https://www.stat.gov.tw/"
ALLOWED_HOSTS = frozenset({"www.stat.gov.tw", "stat.gov.tw", "ws.dgbas.gov.tw"})
OUTPUT_NAME = "dgbas_release_vintages"
PAGE_SIZE = 200
MAX_HTML_BYTES = 2_000_000
MAX_ATTACHMENT_BYTES = 32_000_000


class SourceAccessBlocked(RuntimeError):
    """Official host refused automated access; do not retry a challenge page."""


@dataclass(frozen=True)
class Source:
    name: str
    list_url: str
    required_title_terms: tuple[str, ...]
    grain: str


SOURCES = (
    Source("cpi", "https://www.stat.gov.tw/News.aspx?n=2668&sms=10980",
           ("物價",), "month"),
    Source("unemployment", "https://www.stat.gov.tw/News.aspx?n=2706&sms=11041",
           ("失業", "人力資源"), "month"),
    Source("gdp", "https://www.stat.gov.tw/News.aspx?n=2677&sms=10980",
           ("經濟成長", "國民所得"), "quarter"),
)
SOURCE_BY_NAME = {source.name: source for source in SOURCES}
ROC_DATE_RE = re.compile(r"^(\d{2,3})[-/](\d{1,2})[-/](\d{1,2})$")
MONTH_RE = re.compile(r"(?P<year>\d{2,3})年\s*(?P<month>\d{1,2})月")
QUARTER_RE = re.compile(r"(?P<year>\d{2,3})年\s*第\s*(?P<quarter>[1-4一二三四])\s*季")
CPI_YOY_RE = re.compile(r"消費者物價(?:總)?指數\s*\(CPI\)\s*年增率\s*(?:為)?\s*(?P<direction>漲|跌|升|降)?\s*(?P<sign>-)?(?P<value>\d+(?:\.\d+)?)\s*[%％]")
UNEMPLOYMENT_RE = re.compile(r"(?<!季調)失業率\s*(?:為)?\s*(?P<value>\d+(?:\.\d+)?)\s*[%％]")
PDF_UNEMPLOYMENT_CONTEXT_RE = re.compile(
    r"失業率[^。；;]{0,40}?為\s*(?P<value>\d+(?:\.\d+)?)\s*[%％]"
)
GDP_YOY_RE = re.compile(r"(?:yoy|年增率)\s*(?:為|達)?\s*(?P<value>-?\d+(?:\.\d+)?)\s*[%％]", re.I)
GDP_GROWTH_RE = re.compile(
    r"(?:概估統計\s*)?經濟成長率?\s*(?:概估統計|初步統計)?\s*(?:為|達)?\s*"
    r"(?P<value>-?\d+(?:\.\d+)?)\s*[%％]"
)
PDF_CPI_YOY_RE = re.compile(
    r"(?:與|較)上(?:\(\d{2,3}\))?年同月(?:比較)?[，,\s]*(?:亦|則)?(?:微)?"
    r"(?P<direction>漲|跌|升|降)\s*(?P<value>\d+(?:\.\d+)?)\s*[%％]"
)
PDF_PUBLISHED_RE = re.compile(
    r"中華民國\s*(?P<year>\d{2,3})\s*年\s*(?P<month>\d{1,2})\s*月\s*"
    r"(?P<day>\d{1,2})\s*日\s*(?P<pm>下午|上午)?\s*(?P<hour>\d{1,2})\s*時"
    r"\s*(?:(?P<minute>\d{1,2})\s*分)?\s*發布"
)
ATTACHMENT_EXTENSIONS = frozenset({".pdf", ".xls", ".xlsx", ".ods", ".odt", ".doc", ".docx", ".zip"})
_THREAD_LOCAL = threading.local()
_CA_LOCK = threading.Lock()
_CA_TEMP: tempfile.TemporaryDirectory[str] | None = None
_CA_BUNDLE: str | None = None
TWCA_INTERMEDIATE_URL = "https://sslserver.twca.com.tw/cacert/secure_sha2_2023G3.crt"
TWCA_INTERMEDIATE_SHA256 = "1a2c75fd096e0499e9ff6ac74e526f61eaae3edfc8c2ea4436fee0c24d8b7d0e"


def _roc_date(value: str) -> date:
    match = ROC_DATE_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"invalid official ROC release date: {value!r}")
    return date(int(match[1]) + 1911, int(match[2]), int(match[3]))


def _period(source: Source, title: str) -> str | None:
    # A schedule-change notice may begin with a valid subject period while
    # announcing a future release; it is not that period's value vintage.
    if "提前於" in title and "發布" in title:
        return None
    if not any(term in title for term in source.required_title_terms):
        return None
    match = (MONTH_RE if source.grain == "month" else QUARTER_RE).match(title.strip())
    if match is None:
        return None
    year = int(match["year"]) + 1911
    if source.grain == "month":
        month = int(match["month"])
        return f"{year:04d}-{month:02d}" if 1 <= month <= 12 else None
    quarter = {"一": 1, "二": 2, "三": 3, "四": 4}.get(match["quarter"], match["quarter"])
    return f"{year:04d}-Q{quarter}"


def _headline_value(source: Source, title: str) -> tuple[str | None, float | None]:
    pattern = {"cpi": CPI_YOY_RE, "unemployment": UNEMPLOYMENT_RE, "gdp": GDP_YOY_RE}[source.name]
    # Growth headlines may also mention a forecast. Never let its number
    # substitute for the actually released quarter's GDP observation.
    observed_headline = re.split(r"預測", title, maxsplit=1)[0] if source.name == "gdp" else title
    match = pattern.search(observed_headline)
    if match is None and source.name == "gdp":
        match = GDP_GROWTH_RE.search(observed_headline)
    if match is None:
        return None, None
    value = float(match["value"])
    if source.name == "cpi" and (match["direction"] in {"跌", "降"} or match["sign"] == "-"):
        value = -value
    return {"cpi": "cpi_yoy_pct", "unemployment": "unemployment_rate_pct", "gdp": "gdp_yoy_pct"}[source.name], value


def _pdf_first_page_clock(path: Path, listed_date: str) -> tuple[str, str | None, str | None]:
    """Read the original first page and verify its publication clock/date."""
    reader = PdfReader(path)
    if not reader.pages:
        return "", None, "PDF has no pages"
    text = unicodedata.normalize("NFKC", reader.pages[0].extract_text() or "")
    if not text:
        return "", None, "PDF first page has no extractable text"
    published = PDF_PUBLISHED_RE.search(text[:1200])
    clock: str | None = None
    if published is not None:
        pdf_date = date(
            int(published["year"]) + 1911,
            int(published["month"]), int(published["day"]),
        ).isoformat()
        if pdf_date != listed_date:
            return text, None, f"PDF publication date {pdf_date} differs from official listing {listed_date}"
        hour = int(published["hour"])
        if published["pm"] == "下午" and hour < 12:
            hour += 12
        elif published["pm"] == "上午" and hour == 12:
            hour = 0
        minute = int(published["minute"] or 0)
        if hour > 23 or minute > 59:
            return text, None, "PDF publication clock invalid"
        clock = f"{hour:02d}:{minute:02d}:00"
    return text, clock, None


def _pdf_cpi_evidence(path: Path, listed_date: str) -> tuple[float | None, str | None, str | None]:
    """Extract a first-page CPI comparison only when the original PDF says it."""

    text, clock, warning = _pdf_first_page_clock(path, listed_date)
    if warning is not None:
        return None, None, warning
    cpi_start = re.search(r"消費者物價(?:總)?指數\s*\(CPI\)", text)
    if cpi_start is None:
        return None, clock, "PDF first page lacks identified CPI paragraph"
    # Do not let a later WPI or historical summary table masquerade as the
    # current-period CPI sentence.
    paragraph = text[cpi_start.start():cpi_start.start() + 450]
    paragraph = re.split(r"\n\s*二[、.．]", paragraph, maxsplit=1)[0]
    match = PDF_CPI_YOY_RE.search(paragraph)
    if match is None:
        return None, clock, "PDF first-page CPI year-on-year sentence not recognized"
    value = float(match["value"])
    if match["direction"] in {"跌", "降"}:
        value = -value
    return value, clock, None


def _pdf_unemployment_value(text: str, period: str) -> float | None:
    """Read only the named month's unadjusted headline, never a table/forecast."""

    if not text or not re.fullmatch(r"\d{4}-\d{2}", period):
        return None
    year, month = map(int, period.split("-"))
    compact = re.sub(r"\s+", "", text)
    subject = re.search(
        rf"{year - 1911}年{month}月(?:(?!訂於).){{0,24}}?人力資源調查統計結果(?!訂於)",
        compact[:4000],
    )
    if subject is None:
        return None
    paragraph = compact[subject.end():subject.end() + 1800]
    for match in UNEMPLOYMENT_RE.finditer(paragraph):
        prefix = paragraph[max(0, match.start() - 20):match.start()]
        if ("季調" in prefix and "非季調" not in prefix) or "季節變動因素後" in prefix:
            continue
        value = float(match["value"])
        if 0.0 <= value <= 30.0:
            return value
    for match in PDF_UNEMPLOYMENT_CONTEXT_RE.finditer(paragraph[:700]):
        context = match.group(0)
        if "季調" in context or "季節變動" in context:
            continue
        value = float(match["value"])
        if 0.0 <= value <= 30.0:
            return value
    return None


def _attachment_suffix(url: str) -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path.lower()).suffix
    if suffix not in ATTACHMENT_EXTENSIONS and parsed.path.lower().endswith("/download.ashx"):
        suffix = parse_qs(parsed.query).get("icon", [""])[0].lower()
    return suffix if suffix in ATTACHMENT_EXTENSIONS else ""


def parse_listing(source: Source, content: bytes) -> list[dict[str, str]]:
    soup = BeautifulSoup(content, "html.parser")
    rows: list[dict[str, str]] = []
    for tr in soup.select("tr"):
        title_cell = tr.select_one('td[data-title="標題"]')
        date_cell = tr.select_one('td[data-title="發布日期"]')
        if title_cell is None or date_cell is None:
            continue
        link = title_cell.find("a", href=True)
        if link is None:
            continue
        title = link.get_text(" ", strip=True)
        period = _period(source, title)
        if period is None:
            continue
        url = urljoin(BASE, link["href"])
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
            raise ValueError(f"unexpected DGBAS release URL: {url}")
        if parsed.hostname in {"www.stat.gov.tw", "stat.gov.tw"} and parsed.path == "/News_Content.aspx":
            release_id = parse_qs(parsed.query).get("s", [""])[0]
            if not release_id.isdigit():
                raise ValueError(f"missing release identifier: {url}")
            release_kind = "article"
        elif parsed.hostname == "ws.dgbas.gov.tw" and _attachment_suffix(url):
            release_id = "direct-" + hashlib.sha256(url.encode()).hexdigest()[:16]
            release_kind = "direct_attachment"
        else:
            raise ValueError(f"unsupported DGBAS release target: {url}")
        published_on = _roc_date(date_cell.get_text(" ", strip=True))
        if int(period[:4]) > published_on.year:
            raise ValueError(f"future period in old release: {title}")
        rows.append({"source": source.name, "period": period,
                     "release_id": release_id, "release_url": url,
                     "release_kind": release_kind,
                     "published_on": published_on.isoformat(), "title": title})
    return rows


def parse_detail(content: bytes, listed: dict[str, str]) -> list[str]:
    soup = BeautifulSoup(content, "html.parser")
    article = soup.select_one("#CCMS_Content")
    if article is None:
        raise ValueError(f"DGBAS release {listed['release_id']} lacks article content")
    heading = article.find("h3")
    if heading is None or heading.get_text(" ", strip=True) != listed["title"]:
        raise ValueError(f"DGBAS release title changed for {listed['release_id']}")
    posted = re.search(r"張貼日期\s*[：:]\s*(\d{2,3}[-/]\d{1,2}[-/]\d{1,2})", soup.get_text(" ", strip=True))
    if posted is None or _roc_date(posted[1]).isoformat() != listed["published_on"]:
        raise ValueError(f"DGBAS release date mismatch for {listed['release_id']}")
    urls: list[str] = []
    for link in article.find_all("a", href=True):
        url = urljoin(listed["release_url"], link["href"])
        parsed = urlparse(url)
        if parsed.scheme == "https" and parsed.hostname == "ws.dgbas.gov.tw" and _attachment_suffix(url):
            if url not in urls:
                urls.append(url)
    return urls


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "stockAgent-official-release-archive/1.0", "Accept-Language": "zh-TW,zh;q=0.9"})
        _THREAD_LOCAL.session = session
    return session


def _ws_dgbas_ca_bundle() -> str:
    """Repair the server's missing TWCA intermediate without disabling TLS."""

    global _CA_TEMP, _CA_BUNDLE
    if _CA_BUNDLE is not None:
        return _CA_BUNDLE
    with _CA_LOCK:
        if _CA_BUNDLE is not None:
            return _CA_BUNDLE
        response = requests.get(TWCA_INTERMEDIATE_URL, timeout=15)
        response.raise_for_status()
        der = response.content
        if hashlib.sha256(der).hexdigest() != TWCA_INTERMEDIATE_SHA256:
            raise RuntimeError("TWCA intermediate certificate fingerprint changed")
        bundle = Path(requests.certs.where()).read_bytes()
        bundle += b"\n" + ssl.DER_cert_to_PEM_cert(der).encode("ascii")
        _CA_TEMP = tempfile.TemporaryDirectory(prefix="stockagent-dgbas-ca-")
        atexit.register(_CA_TEMP.cleanup)
        path = Path(_CA_TEMP.name) / "ca-bundle.pem"
        path.write_bytes(bundle)
        _CA_BUNDLE = str(path)
        return _CA_BUNDLE


def _fetch(url: str, limiter: SharedRateLimiter, *, max_bytes: int) -> bytes:
    current = url
    last_response: str | None = None
    for attempt in range(5):
        parsed = urlparse(current)
        if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
            raise ValueError(f"out-of-scope release URL: {current}")
        limiter.wait()
        try:
            verify = _ws_dgbas_ca_bundle() if parsed.hostname == "ws.dgbas.gov.tw" else True
            with _session().get(current, timeout=(10, 35), stream=True,
                                allow_redirects=False, verify=verify) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    last_response = f"HTTP {response.status_code} redirect from {current}"
                    current = urljoin(current, response.headers.get("Location", ""))
                    continue
                if response.status_code in {429, 500, 502, 503, 504}:
                    last_response = f"HTTP {response.status_code} from {current}"
                    if (
                        response.status_code == 429
                        and not response.headers.get("Retry-After")
                        and "cloudflare" in response.headers.get("Server", "").lower()
                    ):
                        raise SourceAccessBlocked(
                            f"official host returned Cloudflare HTTP 429 without Retry-After: {current}"
                        )
                    limiter.defer(retry_delay_seconds(attempt, base=1.0, cap=30.0,
                                                      retry_after=response.headers.get("Retry-After")))
                    continue
                response.raise_for_status()
                size = int(response.headers.get("Content-Length") or 0)
                if size > max_bytes:
                    raise ValueError(f"release body exceeds byte cap: {current}")
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_content(chunk_size=256_000):
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"release body exceeds byte cap: {current}")
                    chunks.append(chunk)
                body = b"".join(chunks)
                if not body:
                    raise ValueError(f"empty release body: {current}")
                return body
        except requests.RequestException as exc:
            last_response = f"{type(exc).__name__}: {exc}"
            if attempt == 4:
                raise
            limiter.defer(retry_delay_seconds(attempt, base=1.0, cap=30.0))
    raise RuntimeError(f"official release fetch exhausted retries: {url}; last={last_response}")


def _save_raw(directory: Path, prefix: str, suffix: str, body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    path = directory / f"{prefix}-{digest[:16]}{suffix}"
    directory.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"immutable release receipt changed: {path}")
    else:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(body)
        os.replace(temporary, path)
    return digest, str(path)


def _cached_detail(directory: Path) -> bytes | None:
    paths = sorted(directory.glob("detail-*.html"), key=lambda path: path.stat().st_mtime_ns)
    if not paths:
        return None
    path = paths[-1]
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest()[:16] != path.stem.removeprefix("detail-"):
        raise RuntimeError(f"cached DGBAS page failed checksum: {path}")
    return body


def _cached_listing(directory: Path, page: int) -> bytes | None:
    paths = sorted(directory.glob(f"page-{page:02d}-*.html"), key=lambda path: path.stat().st_mtime_ns)
    if not paths:
        return None
    path = paths[-1]
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest()[:16] != path.stem.rsplit("-", 1)[-1]:
        raise RuntimeError(f"cached DGBAS listing failed checksum: {path}")
    return body


def _collect_one(listed: dict[str, str], root: Path, limiter: SharedRateLimiter,
                 *, attachments: str, refresh: bool, offline_cache_only: bool = False) -> dict[str, object]:
    source = SOURCE_BY_NAME[listed["source"]]
    directory = root / "raw" / OUTPUT_NAME / source.name / listed["release_id"]
    if listed["release_kind"] == "article":
        body = None if refresh else _cached_detail(directory)
        if body is None:
            if offline_cache_only:
                raise FileNotFoundError(f"no cached original DGBAS article: {listed['release_url']}")
            body = _fetch(listed["release_url"], limiter, max_bytes=MAX_HTML_BYTES)
        html_hash, html_path = _save_raw(directory, "detail", ".html", body)
        try:
            attachment_urls = parse_detail(body, listed)
        except ValueError:
            if refresh or offline_cache_only:
                raise
            body = _fetch(listed["release_url"], limiter, max_bytes=MAX_HTML_BYTES)
            html_hash, html_path = _save_raw(directory, "detail", ".html", body)
            attachment_urls = parse_detail(body, listed)
    else:
        attachment_urls = [listed["release_url"]]
        html_hash, html_path = None, None
    selected = (attachment_urls if listed["release_kind"] == "direct_attachment" or attachments == "all" else
                attachment_urls[:1] if attachments == "primary" else [])
    if attachments == "none" and listed["release_kind"] == "article" and attachment_urls:
        # A no-network pass may still derive facts from an earlier saved,
        # checksum-verified primary attachment.
        selected = attachment_urls[:1]
    attachment_receipts: list[dict[str, str]] = []
    for index, url in enumerate(selected):
        suffix = _attachment_suffix(url)
        prefix = f"attachment-{index:02d}"
        cached = sorted(directory.glob(f"{prefix}-*{suffix}"))
        if cached:
            path = cached[-1]
            payload = path.read_bytes()
            digest = hashlib.sha256(payload).hexdigest()
            if digest[:16] != path.stem.removeprefix(f"{prefix}-"):
                raise RuntimeError(f"cached DGBAS attachment failed checksum: {path}")
            attachment_receipts.append({"url": url, "sha256": digest, "path": str(path)})
            continue
        if attachments == "none" and listed["release_kind"] == "article":
            continue
        if offline_cache_only:
            raise FileNotFoundError(f"no cached original DGBAS attachment: {url}")
        payload = _fetch(url, limiter, max_bytes=MAX_ATTACHMENT_BYTES)
        if suffix == ".pdf" and not payload.startswith(b"%PDF"):
            raise ValueError(f"DGBAS PDF response is not a PDF: {url}")
        digest, path = _save_raw(directory, prefix, suffix, payload)
        attachment_receipts.append({"url": url, "sha256": digest, "path": path})
    metric, value = _headline_value(source, listed["title"])
    value_evidence = "official_release_headline" if metric else "raw_release_only"
    published_clock: str | None = None
    parse_warning: str | None = None
    if attachment_receipts:
        primary = Path(attachment_receipts[0]["path"])
        if primary.suffix.lower() == ".pdf":
            try:
                if source.name == "cpi":
                    pdf_value, published_clock, parse_warning = _pdf_cpi_evidence(
                        primary, listed["published_on"]
                    )
                else:
                    pdf_text, published_clock, parse_warning = _pdf_first_page_clock(
                        primary, listed["published_on"]
                    )
                    pdf_value = (
                        _pdf_unemployment_value(pdf_text, listed["period"])
                        if source.name == "unemployment" and parse_warning is None
                        else None
                    )
            except Exception as exc:
                pdf_value, published_clock = None, None
                parse_warning = f"PDF parse failed: {type(exc).__name__}: {exc}"
            if pdf_value is not None and metric is None:
                metric = {
                    "cpi": "cpi_yoy_pct", "unemployment": "unemployment_rate_pct"
                }[source.name]
                value = pdf_value
                value_evidence = f"original_attachment_{source.name}_text"
    return {
        **listed,
        "published_time_precision": "official_document_time" if published_clock else "official_date_only",
        "published_clock_taipei": published_clock,
        "metric": metric,
        "value_pct": value,
        "value_evidence": value_evidence,
        "parse_warning": parse_warning,
        "html_sha256": html_hash,
        "html_path": html_path,
        "attachment_urls": json.dumps(attachment_urls, ensure_ascii=False),
        "attachment_receipts": json.dumps(attachment_receipts, ensure_ascii=False),
        "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }


@contextmanager
def _writer_lock(root: Path):
    if fcntl is None:
        raise RuntimeError("DGBAS archive requires a POSIX writer lock")
    path = root / "state" / "locks" / f"{OUTPUT_NAME}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another DGBAS release archive writer is active") from exc
        yield


def _write_state(root: Path, payload: dict[str, object]) -> None:
    state = root / "state" / f"{OUTPUT_NAME}.json"
    state.parent.mkdir(parents=True, exist_ok=True)
    temporary = state.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, state)


def collect(root: Path, *, sources: tuple[str, ...], attachments: str,
            workers: int, request_interval: float, refresh_recent: int,
            offline_cache_only: bool = False) -> dict[str, object]:
    if not 0.1 <= request_interval <= 60.0:
        raise ValueError("request_interval must be in [0.1, 60] seconds")
    limiter = SharedRateLimiter(request_interval, name="dgbas-release-archive")
    started = time.monotonic()
    started_at_utc = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    prior_blocked_reason: str | None = None
    if offline_cache_only:
        try:
            previous = json.loads(
                (root / "state" / f"{OUTPUT_NAME}.json").read_text(encoding="utf-8")
            )
            if isinstance(previous, dict) and isinstance(previous.get("source_access_blocked_reason"), str):
                prior_blocked_reason = previous["source_access_blocked_reason"]
        except (OSError, ValueError):
            pass
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running",
                        "phase": "discovering", "started_at_utc": started_at_utc,
                        "completed_releases": 0, "total_releases": None,
                        "estimated_seconds_remaining": None})
    listed: dict[tuple[str, str], dict[str, str]] = {}
    list_receipts: list[dict[str, object]] = []
    for source_index, name in enumerate(sources, start=1):
        source = SOURCE_BY_NAME[name]
        for page in range(1, 100):
            url = f"{source.list_url}&page={page}&PageSize={PAGE_SIZE}"
            body = (
                _cached_listing(root / "raw" / OUTPUT_NAME / "list" / name, page)
                if offline_cache_only else _fetch(url, limiter, max_bytes=MAX_HTML_BYTES)
            )
            if body is None:
                raise FileNotFoundError(f"no cached DGBAS listing page {page} for {name}")
            digest, path = _save_raw(root / "raw" / OUTPUT_NAME / "list" / name,
                                     f"page-{page:02d}", ".html", body)
            rows = parse_listing(source, body)
            # A category page includes non-release commentary; count all table
            # entries to decide whether the last page has been reached.
            raw_count = len(BeautifulSoup(body, "html.parser").select('td[data-title="標題"]'))
            if raw_count == 0 and page == 1:
                raise RuntimeError(f"DGBAS category returned no rows: {source.list_url}")
            list_receipts.append({"source": name, "page": page, "url": url,
                                  "sha256": digest, "path": path, "raw_rows": raw_count,
                                  "release_rows": len(rows)})
            for row in rows:
                key = (name, row["release_id"])
                if key in listed and row != listed[key]:
                    raise ValueError(f"DGBAS listing conflicts for {key}")
                listed[key] = row
            if raw_count < PAGE_SIZE:
                break
        else:
            raise RuntimeError(f"DGBAS category pagination exceeded 99 pages: {name}")
        _write_state(root, {
            "dataset": OUTPUT_NAME, "status": "running", "phase": "discovering",
            "started_at_utc": started_at_utc,
            "completed_sources": source_index, "total_sources": len(sources),
            "discovered_releases": len(listed),
            "completed_releases": 0, "total_releases": None,
            "estimated_seconds_remaining": None,
        })
    ordered = sorted(listed.values(), key=lambda row: (row["source"], row["published_on"], row["release_id"]))
    _write_state(root, {"dataset": OUTPUT_NAME, "status": "running",
                        "phase": "downloading", "started_at_utc": started_at_utc,
                        "completed_releases": 0, "total_releases": len(ordered),
                        "estimated_seconds_remaining": None})
    newest = {name: {row["release_id"] for row in sorted(
        (item for item in ordered if item["source"] == name),
        key=lambda item: item["published_on"], reverse=True)[:refresh_recent]}
        for name in sources}
    results: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    last_progress = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_collect_one, row, root, limiter, attachments=attachments,
                        refresh=row["release_id"] in newest[row["source"]],
                        offline_cache_only=offline_cache_only): row
            for row in ordered
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                if isinstance(exc, SourceAccessBlocked):
                    pool.shutdown(wait=False, cancel_futures=True)
                    raise
                failures.append({"source": row["source"], "release_id": row["release_id"],
                                 "release_url": row["release_url"], "error": f"{type(exc).__name__}: {exc}"})
            completed = len(results) + len(failures)
            now = time.monotonic()
            if now - last_progress >= 5.0 or completed == len(ordered):
                elapsed = max(now - started, 0.001)
                _write_state(root, {"dataset": OUTPUT_NAME, "status": "running",
                                    "phase": "downloading", "started_at_utc": started_at_utc,
                                    "completed_releases": completed,
                                    "total_releases": len(ordered),
                                    "successful_releases": len(results),
                                    "failed_releases": len(failures),
                                    "estimated_seconds_remaining": round(
                                        (len(ordered) - completed) * elapsed / completed
                                    ) if completed else None})
                last_progress = now
    results.sort(key=lambda row: (str(row["source"]), str(row["published_on"]), str(row["release_id"])))
    missing_periods: dict[str, list[str]] = {}
    for name in sources:
        periods = {str(row["period"]) for row in results if row["source"] == name}
        absent: list[str] = []
        if periods:
            if SOURCE_BY_NAME[name].grain == "month":
                first = min(periods)
                last = max(periods)
                year, month = map(int, first.split("-"))
                while f"{year:04d}-{month:02d}" <= last:
                    candidate = f"{year:04d}-{month:02d}"
                    if candidate not in periods:
                        absent.append(candidate)
                    year, month = (year + 1, 1) if month == 12 else (year, month + 1)
            else:
                first = min(periods)
                last = max(periods)
                year, quarter = int(first[:4]), int(first[-1])
                while f"{year:04d}-Q{quarter}" <= last:
                    candidate = f"{year:04d}-Q{quarter}"
                    if candidate not in periods:
                        absent.append(candidate)
                    year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
        missing_periods[name] = absent
    complete = (
        not offline_cache_only and attachments != "none" and not failures
        and len(results) == len(ordered) and not any(missing_periods.values())
    )
    summary: dict[str, object] = {
        "dataset": OUTPUT_NAME, "registered_releases": len(ordered),
        "offline_cache_only": offline_cache_only,
        "source_access_blocked_reason": prior_blocked_reason,
        "saved_releases": len(results), "failures": failures,
        "listing_receipts": list_receipts, "attachment_mode": attachments,
        "source_counts": {name: sum(row["source"] == name for row in results) for name in sources},
        "distinct_periods": {name: len({row["period"] for row in results if row["source"] == name}) for name in sources},
        "latest_period": {name: max((str(row["period"]) for row in results if row["source"] == name), default=None) for name in sources},
        "missing_periods": missing_periods,
        "headline_values": {name: sum(row["source"] == name and row["metric"] is not None for row in results) for name in sources},
        "complete": complete,
        "status": "complete" if complete else "degraded",
        "started_at_utc": started_at_utc,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    # Preserve the prior model-facing archive when any article or attachment
    # failed. Successful raw files are still saved for the next retry.
    if results and not failures:
        destination = root / f"{OUTPUT_NAME}.parquet"
        parquet_sha256, changed = write_release_rows_if_changed(
            destination, results, identity_columns=("source", "release_id")
        )
        summary["parquet_path"] = str(destination)
        summary["parquet_sha256"] = parquet_sha256
        summary["parquet_changed"] = changed
    _write_state(root, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--sources", nargs="+", choices=tuple(SOURCE_BY_NAME), default=tuple(SOURCE_BY_NAME))
    parser.add_argument("--attachments", choices=("none", "primary", "all"), default="primary")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-interval", type=float, default=1.0,
                        help="Host-global spacing in seconds; no numeric official limit is documented. The faster 0.1s trial was blocked by the source.")
    parser.add_argument("--refresh-recent", type=int, default=3,
                        help="Re-fetch newest N releases in each source to capture corrections.")
    parser.add_argument("--offline-cache-only", action="store_true",
                        help="Extract only previously received checksum-verified pages; never contact the official host.")
    args = parser.parse_args()
    if not 1 <= args.workers <= 32 or args.refresh_recent < 0:
        parser.error("workers must be 1..32 and refresh-recent must be nonnegative")
    # Raw receipt paths must be independent of the caller's cwd.  The audit
    # resolves its dataset root, so a relative root here would otherwise
    # encode data_tw_public/raw/... and be joined to that root a second time.
    output_dir = args.output_dir.resolve()
    with _writer_lock(output_dir):
        try:
            summary = collect(output_dir, sources=tuple(args.sources),
                              attachments=args.attachments, workers=args.workers,
                              request_interval=args.request_interval,
                              refresh_recent=0 if args.offline_cache_only else args.refresh_recent,
                              offline_cache_only=args.offline_cache_only)
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
                "source_access_blocked_reason": str(exc) if isinstance(exc, SourceAccessBlocked) else previous.get("source_access_blocked_reason"),
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
