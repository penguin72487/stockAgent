"""Hash-verified public FinMind notice snapshots; never mutate download queues.

``root`` is the dedicated local announcements directory.  Consumers must require
``state == 'ok'`` before planning new work.  A notice, including one saying the
provider repaired data, is not evidence that any local data was repaired.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
import json
from pathlib import Path
import re
import time
import unicodedata
from urllib.parse import quote, urljoin, urlsplit
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag
import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_bytes


SOURCE_URL = "https://finmind.github.io/WhatIsNew/"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
PARSER_VERSION = 1
HTTP_TIMEOUT = (5, 20)
MAX_FETCH_SECONDS = 40
_DATE = re.compile(r"(?<!\d)(20\d{2})[-/](\d{1,2})[-/](\d{1,2})(?!\d)")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
# These are explicit provider ID spellings, not Chinese-name inference.  Taiwan
# prefixes also retain old explicit IDs/aliases for audit; the planner must map
# them to its own supported registry, not assume every token is downloadable.
_DATASET = re.compile(
    r"(?<![A-Za-z0-9_])(?:Taiwan[A-Z][A-Za-z0-9]*|USStock[A-Za-z0-9]*|"
    r"UKStock[A-Za-z0-9]*|JapanStock[A-Za-z0-9]*|EuropeStock[A-Za-z0-9]*|"
    r"GoldPrice|CnnFearGreedIndex|RawMaterialFuturesPrices|CrudeOilPrices|"
    r"GovernmentBondsYield|InterestRate|ExchangeRate|InflationRate|"
    r"TotalMarginPurchaseShortSale|StockStatisticsOfOrderBookAndTrade|"
    r"OptionInstitutionalInvestorsAfterHours|OptionOpenInterestLargeTraders|"
    r"FinancialStatements|BalanceSheet|Shareholding|SecuritiesLending)"
    r"(?![A-Za-z0-9_])"
)
_CORRECTION = re.compile(
    r"校正|更正|修正|修復|已補齊|一併補齊|補回|重新產製|重新計算|重新取得|"
    r"重新下載|已重建|已排除|已移除|\b(?:fix(?:ed|es)?|correct(?:ed|ion)?|repair(?:ed)?)\b",
    re.IGNORECASE,
)
_UNAVAILABLE = re.compile(
    r"無法補齊|無法再次提供|未提供|已知缺漏|無法取得|停止提供|停止更新|"
    r"不再提供|暫停提供|尚未修復|尚未修正|仍有錯誤|資料異常|資料錯誤|"
    r"下架|刪除資料集|移除資料集|\b(?:unavailable|outage|unresolved|deleted|deprecated)\b",
    re.IGNORECASE,
)


class AnnouncementError(ValueError):
    """Only a fixed, credential-free code is surfaced in a status receipt."""


def _normalize(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).split())


def _hash_value(value: object) -> str:
    return sha256_bytes(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":")).encode("utf-8"))


def _text(node: Tag, *, direct: bool = False) -> str:
    return _normalize(" ".join(str(s) for s in node.find_all(string=True)
                               if not direct or s.find_parent("li") is node))


def _dataset_links(node: Tag) -> list[dict]:
    links = []
    for link in node.find_all("a", href=True):
        text = _normalize(link.get_text(" ", strip=True))
        datasets = sorted(set(_DATASET.findall(text)))
        if not datasets:
            continue
        url = urljoin(SOURCE_URL, link["href"])
        parts = urlsplit(url)
        # Links are evidence only: never followed.  Discard untrusted targets,
        # credentials and queries rather than copying a token into receipts.
        if (parts.scheme == "https" and parts.netloc == "finmind.github.io"
                and not parts.query and parts.path.startswith("/tutor/")):
            links.append({"datasets": datasets, "text": text, "url": url})
    return sorted(links, key=lambda item: (item["url"], item["text"]))


def _item_tree(item: Tag, depth: int = 0) -> dict:
    if depth > 32:
        raise AnnouncementError("notice_nesting_too_deep")
    children = [child for child in item.find_all("li")
                if child.find_parent("li") is item]
    text = _text(item)
    return {"text": text, "leading_text": _text(item, direct=True),
            "datasets": sorted(set(_DATASET.findall(text))),
            "dataset_links": _dataset_links(item),
            "nested_items": [_item_tree(child, depth + 1) for child in children]}


def parse_announcements(html: str) -> list[dict]:
    """Parse every dated notice's top-level item, retaining its nested scope.

    Modern dates, old version/date headings and the official 2018 slash dates
    are supported.  The one legacy ``New Function`` h4 inherits the preceding
    dated release explicitly.  Unknown heading shapes fail closed.
    """
    if not isinstance(html, str) or len(html.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise AnnouncementError("html_size_invalid")
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.find_all("article")
    if len(articles) > 1:
        raise AnnouncementError("ambiguous_article")
    content = articles[0] if articles else soup.find("main")
    if content is None:
        raise AnnouncementError("article_missing")
    for excluded in content.find_all(["nav", "footer", "script", "style"]):
        excluded.decompose()
    for permalink in content.select("a.headerlink"):
        permalink.decompose()
    headings = content.find_all("h4")
    if not headings or any(h.parent is not content for h in headings):
        raise AnnouncementError("notice_heading_shape_changed")
    entries: list[dict] = []
    notice_date = None
    for heading in headings:
        title = _normalize(heading.get_text(" ", strip=True))
        dates = _DATE.findall(title)
        if len(dates) == 1:
            try:
                notice_date = date(*(int(part) for part in dates[0])).isoformat()
            except ValueError as exc:
                raise AnnouncementError("notice_date_invalid") from exc
            date_basis = "dated_h4"
        elif not dates and title == "New Function" and notice_date == "2019-04-06":
            date_basis = "preceding_dated_release_h4"
        else:
            raise AnnouncementError("notice_heading_date_missing")
        anchor = heading.get("id")
        if not isinstance(anchor, str) or not anchor or len(anchor) > 256:
            raise AnnouncementError("notice_anchor_missing")
        count = 0
        for sibling in heading.next_siblings:
            if not isinstance(sibling, Tag):
                continue
            if sibling.name in {"h1", "h2", "h3", "h4"}:
                break
            if sibling.name not in {"ul", "ol", "h5", "h6", "hr"}:
                raise AnnouncementError("notice_body_shape_changed")
            if sibling.name not in {"ul", "ol"}:
                continue
            for item in sibling.find_all("li", recursive=False):
                tree = _item_tree(item)
                if not tree["text"]:
                    raise AnnouncementError("notice_item_empty")
                entry_id = _hash_value([notice_date, tree["text"], tree["dataset_links"]])
                # Full semantic identity, not list position.  A family is only
                # a revision hint; it is not a unique task or deduplication key.
                leading = re.split(r"[。；]", tree["leading_text"], maxsplit=1)[0][:160]
                family = _hash_value([notice_date, tree["datasets"], leading])
                correction = bool(_CORRECTION.search(tree["text"]))
                unavailable = bool(_UNAVAILABLE.search(tree["text"]))
                classification = ("mixed" if correction and unavailable else
                                  "is_correction" if correction else
                                  "known_unavailable" if unavailable else "informational")
                entries.append({"entry_id": entry_id, "revision_family_id": family,
                                "notice_date": notice_date, "notice_date_basis": date_basis,
                                "heading": title, "source_url": SOURCE_URL + "#" + quote(anchor, safe="-_"),
                                **tree, "classification": classification,
                                "is_correction": correction, "known_unavailable": unavailable,
                                "local_repair_status": "not_asserted"})
                count += 1
        if not count:
            raise AnnouncementError("notice_items_missing")
    if not entries:
        raise AnnouncementError("notices_missing")
    return entries


def _validate_dates(entries: list[dict], now: datetime) -> None:
    today = now.astimezone(ZoneInfo("Asia/Taipei")).date().isoformat()
    if any(entry["notice_date"] > today for entry in entries):
        raise AnnouncementError("future_notice_date")


def _load_cache(root: Path, now: datetime) -> tuple[dict | None, str | None]:
    try:
        path = root / "head.json"
        if not path.exists():
            return None, None
        if path.stat().st_size > MAX_RESPONSE_BYTES:
            raise AnnouncementError("cache_head_too_large")
        head = json.loads(path.read_text(encoding="utf-8"))
        digest = head.get("snapshot_sha256", "")
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise AnnouncementError("cache_digest_invalid")
        snapshot = root / "snapshots" / f"{digest}.html"
        if snapshot.stat().st_size > MAX_RESPONSE_BYTES:
            raise AnnouncementError("cache_snapshot_too_large")
        payload = snapshot.read_bytes()
        if sha256_bytes(payload) != digest:
            raise AnnouncementError("cache_hash_mismatch")
        entries = parse_announcements(payload.decode("utf-8"))
        _validate_dates(entries, now)
        return {**head, "entries": entries}, None
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        return None, str(exc) if isinstance(exc, AnnouncementError) else "cache_invalid"


def _safe_header(value: object, limit: int = 512) -> str | None:
    return value if (isinstance(value, str) and 0 < len(value) <= limit
                     and all(32 <= ord(char) < 127 for char in value)) else None


def _preserve_snapshot(root: Path, payload: bytes) -> str:
    digest = sha256_bytes(payload)
    path = root / "snapshots" / f"{digest}.html"
    if path.exists():
        if path.stat().st_size != len(payload) or sha256_bytes(path.read_bytes()) != digest:
            raise AnnouncementError("immutable_snapshot_conflict")
    else:
        atomic_write_bytes(path, payload, durable=True)
    return digest


def fetch_announcements(root: Path, now: datetime, session=None) -> dict:
    """One allowlisted conditional GET, verified last-good cache on failure.

    No redirects, retries, auth/session headers/cookies, data APIs or queue
    writes.  The optional session must support Requests' ``send`` interface.
    Failed checks update only ``status.json``; ``head.json`` remains last good.
    Public rejected HTML is retained by SHA for offline diagnosis, not accepted
    as the head.  Use an aware ``now``; notice dates use Asia/Taipei calendar.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now_must_be_timezone_aware")
    root = Path(root)
    checked_at = now.astimezone(UTC).isoformat()
    cache, cache_error = _load_cache(root, now)
    result = {"parser_version": PARSER_VERSION, "source_url": SOURCE_URL,
              "checked_at_utc": checked_at, "state": "degraded", "fetch_status": "failed",
              "snapshot_sha256": cache["snapshot_sha256"] if cache else None,
              "latest_notice_date": max(e["notice_date"] for e in cache["entries"]) if cache else None,
              "entries": cache["entries"] if cache else [], "cache_reused": bool(cache),
              "cache_error": cache_error, "error_code": None}
    client = session if session is not None else requests.Session()
    own_client = session is None
    if own_client:
        client.trust_env = False
    response = None
    try:
        headers = {"Accept": "text/html", "User-Agent": "stockAgent-FinMind-announcements/1"}
        if cache:
            for cache_key, request_key in (("etag", "If-None-Match"),
                                           ("last_modified", "If-Modified-Since")):
                value = _safe_header(cache.get(cache_key))
                if value:
                    headers[request_key] = value
        # Preparing directly intentionally avoids session auth, cookies, default
        # headers and .netrc.  Sending a token-bearing Session must remain safe.
        prepared = requests.Request("GET", SOURCE_URL, headers=headers).prepare()
        started = time.monotonic()
        response = client.send(prepared, allow_redirects=False, stream=True, timeout=HTTP_TIMEOUT)
        if response.url != SOURCE_URL or response.history:
            raise AnnouncementError("response_url_not_allowlisted")
        if response.status_code == 304:
            if not cache:
                raise AnnouncementError("not_modified_without_valid_cache")
            result.update(state="ok", fetch_status="not_modified", cache_reused=True)
        elif response.status_code != 200:
            raise AnnouncementError("redirect_rejected" if 300 <= response.status_code < 400
                                    else "http_status_rejected")
        else:
            media_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if media_type not in {"text/html", "application/xhtml+xml"}:
                raise AnnouncementError("content_type_rejected")
            parts: list[bytes] = []
            size = 0
            for part in response.iter_content(chunk_size=65536):
                size += len(part)
                if size > MAX_RESPONSE_BYTES:
                    raise AnnouncementError("response_too_large")
                if time.monotonic() - started > MAX_FETCH_SECONDS:
                    raise AnnouncementError("fetch_deadline_exceeded")
                parts.append(part)
            payload = b"".join(parts)
            digest = _preserve_snapshot(root, payload)
            result["fetched_snapshot_sha256"] = digest
            try:
                html = payload.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise AnnouncementError("html_encoding_invalid") from exc
            entries = parse_announcements(html)
            _validate_dates(entries, now)
            if cache:
                before = {entry["notice_date"] for entry in cache["entries"]}
                after = {entry["notice_date"] for entry in entries}
                if not before.issubset(after):
                    raise AnnouncementError("historical_notice_sections_disappeared")
            head = {"parser_version": PARSER_VERSION, "source_url": SOURCE_URL,
                    "snapshot_sha256": digest, "fetched_at_utc": checked_at,
                    "latest_notice_date": max(entry["notice_date"] for entry in entries),
                    "etag": _safe_header(response.headers.get("ETag")),
                    "last_modified": _safe_header(response.headers.get("Last-Modified")),
                    "entry_count": len(entries)}
            atomic_write_json(root / "head.json", head)
            result.update(state="ok", fetch_status=("unchanged" if cache and
                          digest == cache["snapshot_sha256"] else "updated"),
                          snapshot_sha256=digest, latest_notice_date=head["latest_notice_date"],
                          entries=entries, cache_reused=False)
    except AnnouncementError as exc:
        result["error_code"] = str(exc)
    except requests.RequestException:
        result["error_code"] = "network_error"
    except (OSError, ValueError, TypeError):
        result["error_code"] = "local_io_or_response_error"
    finally:
        if response is not None:
            response.close()
        if own_client:
            client.close()
    try:
        # Full parsed text stays local, alongside the exact original HTML.
        atomic_write_json(root / "status.json", result)
    except OSError:
        result.update(state="degraded", fetch_status="failed", error_code="status_write_failed")
    return result
