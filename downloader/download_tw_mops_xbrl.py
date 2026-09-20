#!/usr/bin/env python3
"""Discover and preserve MOPS quarterly XBRL archives without inventing PIT history.

The public MOPS page explicitly lists bulk ZIP links.  Automated downloads are
disabled unless the operator records a separate access authorization.  A
manually obtained ZIP may always be ingested locally with ``--mode ingest``.
ToAlpha is deliberately not a fallback here: its terms forbid bulk mirroring.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime
import fcntl
from decimal import Decimal, InvalidOperation
import hashlib
import html
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import tempfile
from xml.etree.ElementTree import ParseError
from urllib.parse import parse_qs, urljoin, urlparse
import zipfile
from zoneinfo import ZoneInfo

from defusedxml import ElementTree as SafeET
from lxml import html as LxmlHTML
import pyarrow as pa
import pyarrow.parquet as pq
import requests

try:
    from downloader.artifact_io import atomic_write_json
    from downloader.common import SharedRateLimiter, resolve_request_interval
except ImportError:  # pragma: no cover - direct execution from downloader/
    from artifact_io import atomic_write_json
    from common import SharedRateLimiter, resolve_request_interval


INDEX_URL = "https://mopsov.twse.com.tw/mops/web/t203sb02"
DOWNLOAD_HOST = "mopsov.twse.com.tw"
FILE_PATTERN = re.compile(r"^(tifrs|tw-gaap)-(20\d{2})Q([1-4])\.zip$")
LINK_PATTERN = re.compile(r"window\.open\(['\"]([^'\"]*FileDownLoad\?[^'\"]+)['\"]", re.I)
XBRLI = "http://www.xbrl.org/2003/instance"
MAX_ARCHIVE_BYTES = 2 * 1024**3
MAX_MEMBER_BYTES = 256 * 1024**2
MAX_TOTAL_UNCOMPRESSED_BYTES = 12 * 1024**3
MAX_MEMBERS = 100_000
TAIPEI = ZoneInfo("Asia/Taipei")
FACT_SCHEMA = pa.schema([(name, pa.string()) for name in (
    "archive_sha256", "archive_name", "source_member", "document_sha256",
    "document_type", "concept", "context_ref", "entity_identifier",
    "period_start", "period_end", "period_instant", "report_period_end", "dimensions_json",
    "unit_ref", "decimals", "scale", "sign", "raw_value", "decimal_value",
    "value_parse_status", "first_observed_at_utc", "published_at_utc",
)])


@dataclass(frozen=True, order=True)
class ArchiveLink:
    year: int
    quarter: int
    standard: str
    filename: str
    url: str

    @property
    def period(self) -> str:
        return f"{self.year}Q{self.quarter}"


class ArchiveUnavailable(RuntimeError):
    """An advertised quarter is not downloadable yet; keep it missing."""


def _work_queue(links: list[ArchiveLink], missing_keys: set[str],
                attempts: dict[str, dict[str, str]], recheck_recent_quarters: int) -> list[ArchiveLink]:
    """Refresh the newest quarters, then rotate old gaps by least-recent attempt."""
    recent = list(reversed(links[-recheck_recent_quarters:])) if recheck_recent_quarters else []
    missing = sorted(
        (link for link in links if f"{link.standard}:{link.period}" in missing_keys),
        key=lambda link: (str(attempts.get(f"{link.standard}:{link.period}", {}).get("at_utc") or ""),
                          link.year, link.quarter),
    )
    return recent + [link for link in missing if link not in recent]


def _selected_work(links: list[ArchiveLink], missing_keys: set[str],
                   attempts: dict[str, dict[str, str]], *,
                   recheck_recent_quarters: int, max_archives: int,
                   backfill_all: bool) -> list[ArchiveLink]:
    """Select one resumable pass; a full backfill never retries a gap in the same pass."""
    queue = _work_queue(links, missing_keys, attempts, recheck_recent_quarters)
    return queue if backfill_all else queue[:max_archives]


def _quarter_end(year: int, quarter: int) -> date:
    return date(
        year, (3, 6, 9, 12)[quarter - 1], (31, 30, 30, 31)[quarter - 1]
    )


def discover_archives(page: str, *, today: date | None = None) -> list[ArchiveLink]:
    """Use only exact links advertised by the official page; never guess ZIP URLs."""
    if "案例文件整批下載" not in page:
        raise ValueError("MOPS did not return the quarterly bulk-download page")
    cutoff = today or datetime.now(TAIPEI).date()
    found: dict[tuple[str, int, int], ArchiveLink] = {}
    for href in LINK_PATTERN.findall(page):
        url = urljoin(INDEX_URL, html.unescape(href))
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname != DOWNLOAD_HOST:
            raise ValueError("MOPS archive link escaped the expected HTTPS host")
        if parsed.path != "/server-java/FileDownLoad":
            raise ValueError("MOPS archive link uses an unexpected path")
        query = parse_qs(parsed.query, keep_blank_values=True)
        if set(query) != {"step", "functionName", "fileName", "filePath"}:
            raise ValueError("MOPS archive link uses unexpected parameters")
        if query["step"] != ["9"] or query["functionName"] != ["show_file2"]:
            raise ValueError("MOPS archive link uses an unexpected download function")
        filename = query["fileName"][0]
        match = FILE_PATTERN.fullmatch(filename)
        if not match:
            raise ValueError(f"MOPS archive name changed: {filename!r}")
        prefix, year_text, quarter_text = match.groups()
        year, quarter = int(year_text), int(quarter_text)
        standard = "ifrs" if prefix == "tifrs" else "tw_gaap"
        expected_path = f"/{'ifrs' if standard == 'ifrs' else 'xbrl'}/{year}/"
        if query["filePath"] != [expected_path]:
            raise ValueError(f"MOPS archive directory changed: {filename!r}")
        if _quarter_end(year, quarter) >= cutoff:
            continue  # The page can advertise a future Q4 ZIP before publication.
        key = (standard, year, quarter)
        entry = ArchiveLink(year, quarter, standard, filename, url)
        if key in found and found[key] != entry:
            raise ValueError(f"conflicting official links for {key}")
        found[key] = entry
    if not found:
        raise ValueError("MOPS page advertised no completed quarterly archives")
    return sorted(found.values())


def _safe_members(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    members = archive.infolist()
    if not members or len(members) > MAX_MEMBERS:
        raise ValueError("empty or excessive XBRL ZIP member count")
    total = 0
    seen: set[str] = set()
    for member in members:
        path = PurePosixPath(member.filename)
        if (not member.filename or member.filename.startswith("/") or "\\" in member.filename
                or ".." in path.parts or path.is_absolute() or member.flag_bits & 1):
            raise ValueError(f"unsafe XBRL ZIP member: {member.filename!r}")
        if member.filename in seen:
            raise ValueError(f"duplicate XBRL ZIP member: {member.filename!r}")
        seen.add(member.filename)
        if (member.external_attr >> 16) & 0o170000 == 0o120000:
            raise ValueError(f"XBRL ZIP symlink is not allowed: {member.filename!r}")
        if member.file_size > MAX_MEMBER_BYTES:
            raise ValueError(f"XBRL ZIP member is too large: {member.filename!r}")
        total += member.file_size
        if total > MAX_TOTAL_UNCOMPRESSED_BYTES:
            raise ValueError("XBRL ZIP expands beyond the configured safety limit")
    return members


def validate_zip(path: Path) -> dict[str, int]:
    if not path.is_file() or path.stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError("XBRL archive missing or beyond configured byte limit")
    with zipfile.ZipFile(path) as archive:
        members = _safe_members(archive)
        for member in members:
            if member.is_dir():
                continue
            with archive.open(member) as stream:
                while stream.read(1 << 20):
                    pass  # CRC and decompression must pass before publication.
        return {"member_count": len(members),
                "uncompressed_bytes": sum(item.file_size for item in members)}


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _context_map(root, *, html_mode: bool = False) -> dict[str, dict[str, str | None]]:
    def is_xbrli(node, name: str) -> bool:
        if not isinstance(node.tag, str):
            return False
        return (node.tag == f"xbrli:{name.lower()}" if html_mode
                else node.tag == f"{{{XBRLI}}}{name}")

    result: dict[str, dict[str, str | None]] = {}
    for node in root.iter():
        if not is_xbrli(node, "context"):
            continue
        identifier = next(("".join(part.itertext()).strip() for part in node.iter()
                           if is_xbrli(part, "identifier")), None)
        values = {name: next(("".join(part.itertext()).strip() for part in node.iter()
                              if is_xbrli(part, name)), None)
                  for name in ("startDate", "endDate", "instant")}
        dimensions = [
            {"axis": part.attrib.get("dimension"), "member": "".join(part.itertext()).strip()}
            for part in node.iter() if isinstance(part.tag, str)
            and _local_name(part.tag).lower() in {"explicitmember", "typedmember"}
        ]
        result[node.attrib.get("id", "")] = {
            "entity_identifier": identifier,
            "period_start": values["startDate"],
            "period_end": values["endDate"],
            "period_instant": values["instant"],
            "dimensions_json": json.dumps(dimensions, ensure_ascii=False, sort_keys=True),
        }
    return result


def _decimal_value(raw: str, *, scale: str | None, sign: str | None,
                   format_name: str | None) -> tuple[str | None, str]:
    if format_name and not format_name.lower().endswith("numdotdecimal"):
        return None, "unsupported_inline_format"
    lexical = raw.replace(",", "") if format_name else raw
    try:
        value = Decimal(lexical)
        if not value.is_finite():
            raise InvalidOperation
        exponent = int(scale or "0")
        if not -30 <= exponent <= 30:
            raise ValueError("scale out of range")
        value = value.scaleb(exponent)
        if sign == "-":
            value = -value
        elif sign not in {None, "", "+"}:
            return None, "unsupported_sign"
        return format(value, "f"), "parsed"
    except (InvalidOperation, ValueError):
        return None, "not_plain_decimal"


def parse_document(payload: bytes, *, archive_name: str, archive_sha256: str,
                   member_name: str, observed_at: str,
                   recovery_events: list[dict[str, object]] | None = None,
                   ) -> list[dict[str, str | None]]:
    # Some official quarterly ZIPs contain saved HTML 4 inline XBRL reports.
    # Try strict XML first. Only malformed HTML with an inline-XBRL header
    # receives HTML recovery, with network access disabled.
    header = payload[:4096]
    html_inline = (member_name.lower().endswith((".html", ".htm"))
                   and b"inlineXBRL" in header
                   and re.search(br"<html\b", header, re.I) is not None)
    # Expat accepts UTF-8 XML bytes but cannot decode historical BIG5 itself.
    # Decode only an explicitly declared BIG5 document; retain raw bytes for
    # its SHA-256 and keep defusedxml's entity/external-reference protections.
    declaration = re.match(br"^\s*<\?xml\s+[^>]*\?>", payload[:512], re.I)
    encoding = (re.search(br"\bencoding\s*=\s*['\"]([^'\"]+)['\"]",
                          declaration.group(0), re.I) if declaration else None)
    source: bytes | str = payload
    if encoding and encoding.group(1).lower() in {b"big5", b"big-5"}:
        source = payload.decode("big5")
    html_mode = False
    unresolved_prefixes: set[str] = set()
    try:
        root = SafeET.fromstring(source, forbid_dtd=False, forbid_entities=True,
                                forbid_external=True)
    except ParseError as exc:
        if html_inline:
            root = LxmlHTML.fromstring(payload, parser=LxmlHTML.HTMLParser(no_network=True))
            html_mode = True
            if recovery_events is not None:
                recovery_events.append({"member": member_name, "type": "saved_html_inline_xbrl"})
        elif "unbound prefix" in str(exc) and isinstance(source, str):
            # One historical filing uses a QName prefix without declaring its
            # URI. Bind a temporary private URI solely to permit strict XML
            # parsing; emit the original lexical QName, not an invented URI.
            used = set(re.findall(r"</?([A-Za-z_][\w.-]*):[A-Za-z_]", source))
            declared = set(re.findall(r"\bxmlns:([A-Za-z_][\w.-]*)\s*=", source))
            unresolved_prefixes = used - declared
            root_match = re.search(r"<xbrl\b[^>]*>", source[:8192], re.I | re.S)
            if not root_match or not 1 <= len(unresolved_prefixes) <= 4:
                raise
            bindings = "".join(
                f' xmlns:{prefix}="urn:stockagent:unresolved-prefix:{prefix}"'
                for prefix in sorted(unresolved_prefixes)
            )
            repaired = (source[:root_match.end() - 1] + bindings
                        + source[root_match.end() - 1:])
            root = SafeET.fromstring(repaired, forbid_dtd=False,
                                    forbid_entities=True, forbid_external=True)
            if recovery_events is not None:
                recovery_events.append({"member": member_name,
                                        "type": "undeclared_xml_prefix",
                                        "prefixes": sorted(unresolved_prefixes)})
        else:
            raise
    contexts = _context_map(root, html_mode=html_mode)
    document_sha = hashlib.sha256(payload).hexdigest()
    rows: list[dict[str, str | None]] = []
    for node in root.iter():
        if not isinstance(node.tag, str):
            continue
        context_ref = node.attrib.get("contextRef") or node.attrib.get("contextref")
        if not context_ref:
            continue
        inline = _local_name(node.tag).lower() in {"nonfraction", "nonnumeric"}
        concept = node.attrib.get("name") if inline else node.tag
        if concept and not inline:
            for prefix in unresolved_prefixes:
                marker = f"{{urn:stockagent:unresolved-prefix:{prefix}}}"
                if concept.startswith(marker):
                    concept = f"{prefix}:{concept[len(marker):]}"
                    break
        raw = "".join(node.itertext()).strip()
        if not concept or not raw:
            continue
        unit = node.attrib.get("unitRef") or node.attrib.get("unitref")
        scale = node.attrib.get("scale")
        sign = node.attrib.get("sign")
        fmt = node.attrib.get("format")
        decimal, parse_status = (
            _decimal_value(raw, scale=scale, sign=sign, format_name=fmt)
            if unit or _local_name(node.tag).lower() == "nonfraction"
            else (None, "non_numeric")
        )
        context = contexts.get(context_ref, {})
        rows.append({
            "archive_sha256": archive_sha256,
            "archive_name": archive_name,
            "source_member": member_name,
            "document_sha256": document_sha,
            "document_type": "inline_xbrl" if inline else "xbrl_xml",
            "concept": concept,
            "context_ref": context_ref,
            "entity_identifier": context.get("entity_identifier"),
            "period_start": context.get("period_start"),
            "period_end": context.get("period_end"),
            "period_instant": context.get("period_instant"),
            "report_period_end": context.get("period_end") or context.get("period_instant"),
            "dimensions_json": context.get("dimensions_json"),
            "unit_ref": unit,
            "decimals": node.attrib.get("decimals"),
            "scale": scale,
            "sign": sign,
            "raw_value": raw,
            "decimal_value": decimal,
            "value_parse_status": parse_status,
            "first_observed_at_utc": observed_at,
            "published_at_utc": None,  # A reporting period is not a filing timestamp.
        })
    return rows


def _hash_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _normalize_archive(path: Path, *, archive_name: str, archive_sha256: str,
                       observed_at: str, output_dir: Path,
                       ) -> tuple[int, int, list[dict[str, object]]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary = output_dir / f".facts.{os.getpid()}.tmp"
    writer: pq.ParquetWriter | None = None
    fact_count = document_count = 0
    recovery_events: list[dict[str, object]] = []
    batch: list[dict[str, str | None]] = []
    try:
        with zipfile.ZipFile(path) as archive:
            for member_name, payload in _document_payloads(archive):
                rows = parse_document(payload, archive_name=archive_name,
                                      archive_sha256=archive_sha256,
                                      member_name=member_name,
                                      observed_at=observed_at,
                                      recovery_events=recovery_events)
                document_count += 1
                fact_count += len(rows)
                batch.extend(rows)
                if len(batch) >= 50_000:
                    if writer is None:
                        writer = pq.ParquetWriter(temporary, FACT_SCHEMA, compression="zstd")
                    writer.write_table(pa.Table.from_pylist(batch, schema=FACT_SCHEMA))
                    batch.clear()
        if not fact_count:
            raise ValueError("XBRL archive contained no parseable facts")
        if writer is None:
            writer = pq.ParquetWriter(temporary, FACT_SCHEMA, compression="zstd")
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=FACT_SCHEMA))
        writer.close()
        writer = None
        os.replace(temporary, output_dir / "facts.parquet")
        return document_count, fact_count, recovery_events
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)


def _document_payloads(archive: zipfile.ZipFile):
    """Yield XML/iXBRL from a quarterly ZIP or one level of company ZIPs."""
    remaining = MAX_TOTAL_UNCOMPRESSED_BYTES
    for member in _safe_members(archive):
        if member.is_dir():
            continue
        name = member.filename
        if name.lower().endswith((".xml", ".xbrl", ".xhtml", ".html", ".htm")):
            remaining -= member.file_size
            if remaining < 0:
                raise ValueError("XBRL documents exceed the total expansion limit")
            yield name, archive.read(member)
        elif name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(archive.read(member))) as child:
                for submember in _safe_members(child):
                    if submember.is_dir() or not submember.filename.lower().endswith(
                        (".xml", ".xbrl", ".xhtml", ".html", ".htm")
                    ):
                        continue
                    remaining -= submember.file_size
                    if remaining < 0:
                        raise ValueError("nested XBRL documents exceed the total expansion limit")
                    yield f"{name}!{submember.filename}", child.read(submember)


def _ingest(path: Path, *, root: Path, source_url: str | None,
            source_authenticity_verified: bool = False,
            move_staged_source: bool = False) -> dict[str, object]:
    match = FILE_PATTERN.fullmatch(path.name)
    if not match:
        raise ValueError("input filename is not an advertised MOPS quarterly ZIP name")
    prefix, year_text, quarter_text = match.groups()
    year, quarter = int(year_text), int(quarter_text)
    standard = "ifrs" if prefix == "tifrs" else "tw_gaap"
    if _quarter_end(year, quarter) >= datetime.now(TAIPEI).date():
        raise ValueError("future or current uncompleted quarter is not ingestible")
    stats = validate_zip(path)
    digest = _hash_file(path)
    period = f"{year}Q{quarter}"
    destination_dir = root / "raw" / standard / period
    destination = destination_dir / f"{digest}.zip"
    receipt_path = destination_dir / f"{digest}.json"
    if destination.is_file() and _hash_file(destination) != digest:
        raise ValueError("stored XBRL archive hash mismatch")
    observed_at = datetime.now(UTC).isoformat()
    if receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        observed_at = str(receipt["first_observed_at_utc"])
        source_authenticity_verified = (
            source_authenticity_verified or receipt.get("source_authenticity_verified") is True
        )
        source_url = source_url or receipt.get("source_url")
    if not destination.is_file():
        destination_dir.mkdir(parents=True, exist_ok=True)
        if move_staged_source:
            os.replace(path, destination)
        else:
            with tempfile.NamedTemporaryFile(prefix=".xbrl.", suffix=".tmp",
                                             dir=destination_dir, delete=False) as handle:
                temporary = Path(handle.name)
            try:
                with path.open("rb") as source, temporary.open("wb") as target:
                    while chunk := source.read(1 << 20):
                        target.write(chunk)
                if _hash_file(temporary) != digest:
                    raise ValueError("XBRL archive changed during copy")
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
    normalized_dir = root / "normalized" / standard / period / digest
    normalized = normalized_dir / "facts.parquet"
    if normalized.is_file() and receipt_path.is_file():
        previous_facts_sha = receipt.get("facts_sha256")
        if previous_facts_sha and _hash_file(normalized) != previous_facts_sha:
            raise ValueError("stored normalized XBRL facts hash mismatch")
        fact_count = pq.ParquetFile(normalized).metadata.num_rows
        document_count = int(receipt.get("document_count", 0))
        recovery_events = list(receipt.get("parse_recovery_events", []))
    else:
        document_count, fact_count, recovery_events = _normalize_archive(
            destination, archive_name=path.name, archive_sha256=digest,
            observed_at=observed_at, output_dir=normalized_dir,
        )
    receipt = {
        "source": "MOPS official XBRL quarterly bulk ZIP",
        "source_url": source_url,
        "source_authenticity_verified": source_authenticity_verified,
        "standard": standard,
        "period": period,
        "archive_name": path.name,
        "archive_sha256": digest,
        "archive_bytes": destination.stat().st_size,
        **stats,
        "document_count": document_count,
        "fact_count": fact_count,
        "parse_recovery_events": recovery_events,
        "facts_sha256": _hash_file(normalized),
        "first_observed_at_utc": observed_at,
        "published_at_utc": None,
        "historical_point_in_time": False,
        "training_eligible": False,
        "raw_path": str(destination),
        "facts_path": str(normalized),
    }
    atomic_write_json(receipt_path, receipt)
    return receipt


def _authorization(path: Path | None) -> date:
    if path is None or not path.is_file():
        raise PermissionError("automated MOPS XBRL download requires --authorization-file")
    item = json.loads(path.read_text(encoding="utf-8"))
    try:
        expires_on = date.fromisoformat(str(item.get("expires_on")))
    except (AttributeError, TypeError, ValueError) as exc:
        raise PermissionError("MOPS XBRL authorization expiry is invalid") from exc
    if (item.get("provider") != "TWSE"
            or item.get("scope") != "automated_mops_xbrl_bulk_download"
            or item.get("authorized") is not True
            or not str(item.get("evidence_reference") or "").strip()
            or expires_on < datetime.now(TAIPEI).date()):
        raise PermissionError("MOPS XBRL authorization attestation is absent, invalid or expired")
    return expires_on


def summarize(links: list[ArchiveLink], root: Path, *, authorized: bool,
              authorization_expires_on: date | None = None,
              attempts: dict[str, dict[str, str]] | None = None) -> dict[str, object]:
    """Report archive coverage, never equating discovered links to completed data."""
    completed: list[str] = []
    missing: list[str] = []
    versions = 0
    facts = 0
    local_completed: list[str] = []
    local_missing: list[str] = []
    local_versions = 0
    local_facts = 0
    for link in links:
        key = f"{link.standard}:{link.period}"
        receipts = sorted((root / "raw" / link.standard / link.period).glob("*.json"))
        good = False
        local_good = False
        for receipt_path in receipts:
            try:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                digest = str(receipt["archive_sha256"])
                normalized = root / "normalized" / link.standard / link.period / digest / "facts.parquet"
                if (receipt_path.stem != digest or not receipt_path.with_suffix(".zip").is_file()
                        or not normalized.is_file()):
                    continue
                row_count = int(receipt["fact_count"])
                local_good = True
                local_versions += 1
                local_facts += row_count
                if receipt.get("source_authenticity_verified") is True:
                    good = True
                    versions += 1
                    facts += row_count
            except (OSError, ValueError, KeyError, TypeError):
                continue
        (completed if good else missing).append(key)
        (local_completed if local_good else local_missing).append(key)
    return {
        "source": "MOPS official quarterly XBRL ZIP",
        "index_url": INDEX_URL,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "status": "blocked_authorization" if not authorized else "incomplete" if missing else "complete_archive_coverage",
        "automated_download_authorized": authorized,
        "authorization_expires_on": authorization_expires_on.isoformat() if authorization_expires_on else None,
        "discovered_periods": len(links),
        "completed_periods": len(completed),
        "missing_periods": missing,
        "local_imported_periods": len(local_completed),
        "local_missing_periods": local_missing,
        "local_import_complete": not local_missing,
        "local_archive_versions": local_versions,
        "local_fact_rows": local_facts,
        "advertised_periods": [f"{link.standard}:{link.period}" for link in links],
        "archive_versions": versions,
        "fact_rows": facts,
        "attempts": attempts or {},
        "earliest_period": f"{links[0].standard}:{links[0].period}" if links else None,
        "latest_period": f"{links[-1].standard}:{links[-1].period}" if links else None,
        "historical_point_in_time": False,
        "training_eligible": False,
        "toalpha_bulk_fallback": "blocked_by_provider_terms",
        "coverage_basis": "advertised completed-quarter links versus matching local archive/Parquet receipts; completed_periods additionally requires verified source transport; no per-scan rehash or company-level proof",
    }


def _fetch_index(session: requests.Session, limiter: SharedRateLimiter) -> list[ArchiveLink]:
    limiter.wait()
    response = session.get(INDEX_URL, timeout=30, allow_redirects=False)
    if response.status_code != 200:
        raise RuntimeError(f"MOPS archive index returned HTTP {response.status_code}")
    return discover_archives(response.text)


def _download_one(session: requests.Session, limiter: SharedRateLimiter,
                  link: ArchiveLink, root: Path) -> dict[str, object]:
    limiter.wait()
    with session.get(link.url, stream=True, timeout=(20, 180), allow_redirects=False) as response:
        if response.status_code in {307, 404}:
            raise ArchiveUnavailable(f"advertised archive is not available: HTTP {response.status_code}")
        if response.status_code in {403, 429}:
            raise RuntimeError(f"MOPS access/rate guard returned HTTP {response.status_code}; stop without bypass")
        response.raise_for_status()
        media_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if media_type in {"text/html", "text/plain", "application/json"}:
            raise ValueError("MOPS returned a non-ZIP content type")
        disposition = response.headers.get("Content-Disposition", "")
        filename_match = re.search(r"(?:^|;)\s*filename\s*=\s*\"?([^\";]+)", disposition, re.I)
        if filename_match is None or filename_match.group(1).strip() != link.filename:
            raise ValueError("MOPS ZIP filename did not match the advertised link")
        with tempfile.TemporaryDirectory(prefix=".stockagent-xbrl-", dir=root) as folder:
            temporary = Path(folder) / link.filename
            size = 0
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    size += len(chunk)
                    if size > MAX_ARCHIVE_BYTES:
                        raise ValueError("MOPS ZIP exceeded configured byte limit")
                    handle.write(chunk)
            return _ingest(temporary, root=root, source_url=link.url,
                           source_authenticity_verified=True, move_staged_source=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("discover", "ingest", "download"), required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public/mops_xbrl"))
    parser.add_argument("--input", type=Path, help="Manually obtained official quarterly ZIP")
    parser.add_argument("--authorization-file", type=Path,
                        help="Operator attestation of TWSE automated-download authorization")
    parser.add_argument("--max-archives", type=int, default=1,
                        help="Bound each authorized run; default one archive")
    parser.add_argument("--backfill-all", action="store_true",
                        help="In one authorized run, try every missing advertised quarter once")
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--write-state", action="store_true",
                        help="In discover mode, persist a coverage snapshot for the data monitor")
    parser.add_argument("--recheck-recent-quarters", type=int, default=2,
                        help="Authorized runs recheck this many latest periods for archive revisions")
    args = parser.parse_args()
    args.output_dir = args.output_dir.resolve()
    if args.max_archives < 1 or args.max_archives > 16:
        parser.error("--max-archives must be in 1..16")
    if args.backfill_all and args.mode != "download":
        parser.error("--backfill-all requires --mode download")
    if args.recheck_recent_quarters < 0 or args.recheck_recent_quarters > 16:
        parser.error("--recheck-recent-quarters must be in 0..16")
    if args.mode == "ingest":
        if args.input is None:
            parser.error("--mode ingest requires --input")
        print(json.dumps(_ingest(args.input, root=args.output_dir, source_url=None),
                         ensure_ascii=False, indent=2))
        return 0
    authorized = args.mode == "download"
    authorization_expires_on = None
    if authorized:
        authorization_expires_on = _authorization(args.authorization_file)
    limiter = SharedRateLimiter(
        resolve_request_interval("tw_public", args.request_interval), name="tw_public"
    )
    with requests.Session() as session:
        session.headers.update({"User-Agent": "stockAgent/1.0 (research archive)",
                                "Accept": "text/html,application/zip,*/*"})
        links = _fetch_index(session, limiter)
        if args.mode == "discover":
            coverage = summarize(links, args.output_dir, authorized=False)
            if args.write_state:
                args.output_dir.mkdir(parents=True, exist_ok=True)
                atomic_write_json(args.output_dir / "state.json", coverage)
            print(json.dumps({"coverage": coverage,
                              "links": [link.__dict__ for link in links]}, ensure_ascii=False, indent=2))
            return 0
        lock_path = args.output_dir.parent.parent / ".locks" / "tw-public-refresh.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            args.output_dir.mkdir(parents=True, exist_ok=True)
            previous_state_path = args.output_dir / "state.json"
            try:
                previous_state = json.loads(previous_state_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                previous_state = {}
            attempts = previous_state.get("attempts", {}) if isinstance(previous_state, dict) else {}
            attempts = ({str(key): value for key, value in attempts.items()
                         if isinstance(value, dict)} if isinstance(attempts, dict) else {})
            status = summarize(links, args.output_dir, authorized=True,
                               authorization_expires_on=authorization_expires_on,
                               attempts=attempts)
            status["status"] = "updating"
            atomic_write_json(args.output_dir / "state.json", status)
            missing_keys = set(status["missing_periods"])
            queue = _selected_work(
                links, missing_keys, attempts,
                recheck_recent_quarters=args.recheck_recent_quarters,
                max_archives=args.max_archives, backfill_all=args.backfill_all,
            )
            downloaded = 0
            attempted = 0
            try:
                for link in queue:
                    key = f"{link.standard}:{link.period}"
                    attempted += 1
                    try:
                        receipt = _download_one(session, limiter, link, args.output_dir)
                    except ArchiveUnavailable as exc:
                        attempts[key] = {"at_utc": datetime.now(UTC).isoformat(),
                                         "outcome": "source_unavailable", "detail": str(exc)}
                    else:
                        attempts[key] = {"at_utc": datetime.now(UTC).isoformat(),
                                         "outcome": "downloaded", "sha256": str(receipt["archive_sha256"])}
                        print(json.dumps(receipt, ensure_ascii=False), flush=True)
                        downloaded += 1
                    status = summarize(links, args.output_dir, authorized=True,
                                       authorization_expires_on=authorization_expires_on,
                                       attempts=attempts)
                    status["status"] = "updating"
                    atomic_write_json(args.output_dir / "state.json", status)
            except Exception as exc:
                status = summarize(links, args.output_dir, authorized=True,
                                   authorization_expires_on=authorization_expires_on,
                                   attempts=attempts)
                status["status"] = "failed"
                status["last_error"] = f"{type(exc).__name__}: {exc}"
                atomic_write_json(args.output_dir / "state.json", status)
                raise
            status = summarize(links, args.output_dir, authorized=True,
                               authorization_expires_on=authorization_expires_on,
                               attempts=attempts)
            atomic_write_json(args.output_dir / "state.json", status)
            print(json.dumps({"attempted": attempted, "downloaded": downloaded,
                              "coverage": status}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, PermissionError, zipfile.BadZipFile) as exc:
        print(f"XBRL acquisition blocked: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
