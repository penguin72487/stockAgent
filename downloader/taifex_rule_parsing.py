"""Bounded, provenance-preserving extraction of official TAIFEX rule documents.

This module parses bytes only. It never fetches URLs, supplies a publication date
from a filename, or promotes a current table into a historical rule. In particular,
PDF text extraction is not a verified reconstruction of its tabular amounts.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree

PARSER_VERSION = 2
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
MAX_PDF_PAGES = 80
MAX_TEXT_CHARACTERS = 2_000_000
MAX_TABLE_ROWS = 100_000
MAX_TABLE_COLUMNS = 256
MAX_ZIP_MEMBERS = 256
MAX_OFFICE_XML_BYTES = 4 * 1024 * 1024

_WORD = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_ODF_OFFICE = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"
_ODF_TABLE = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
_ODF_TEXT = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"

_DATE_PART = r"[0-9〇零一二三四五六七八九十百千兩]{1,4}"
_DATE = re.compile(
    rf"(?<![0-9])(?P<era>中華民國|民國)?\s*(?P<year>{_DATE_PART})\s*"
    rf"(?:年|[/-])\s*(?P<month>{_DATE_PART})\s*(?:月|[/-])\s*"
    rf"(?P<day>{_DATE_PART})\s*日?(?![0-9])"
)
_PUNCTUATION = re.compile(r"[。；;\n\r]")
_MARGIN_HEADERS = {
    f"調整{side}{label}保證金": (phase, metric)
    for side, phase in (("前", "before"), ("後", "after"))
    for label, metric in (("原始", "initial"), ("維持", "maintenance"), ("結算", "clearing"))
}


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def _decode(content: bytes) -> str:
    """Do not replace damaged bytes with plausible but incorrect characters."""
    for encoding in ("utf-8-sig", "cp950"):
        try:
            return content.decode(encoding, errors="strict")
        except UnicodeDecodeError:
            pass
    raise ValueError("unsupported_or_damaged_text_encoding")


def _integer(token: str) -> int:
    if token.isascii() and token.isdigit():
        return int(token)
    digits = dict(zip("零〇一二三四五六七八九兩", (0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 2)))
    if not any(char in token for char in "十百千"):
        return int("".join(str(digits[char]) for char in token))
    total = digit = 0
    for char in token:
        if char in digits:
            digit = digits[char]
        else:
            total += (digit or 1) * {"十": 10, "百": 100, "千": 1000}[char]
            digit = 0
    return total + digit


def temporal_mentions(text: str) -> list[dict[str, str]]:
    """Find explicit dated clauses; these are evidence candidates, not a clock.

    A bare date or a citation stays unspecified/reference. Effective dates require
    adjacent implementation language; a publication date never implies midnight
    effectiveness. Session boundaries retain the wording without guessing a time.
    """
    normalized = unicodedata.normalize("NFKC", text)
    # PDF line wrapping also splits a closing parenthesis from the session
    # boundary. Dates here are Chinese legal clauses, not whitespace tokens.
    normalized = re.sub(r"\s+", "", normalized)
    matches = list(_DATE.finditer(normalized))
    result: list[dict[str, str]] = []
    for index, match in enumerate(matches):
        try:
            year, month, day = (_integer(match[name]) for name in ("year", "month", "day"))
            if match["era"] or year < 1911:
                year += 1911
            value = date(year, month, day).isoformat()
        except (ValueError, KeyError):
            continue
        start = max(0, match.start() - 65)
        end = min(len(normalized), match.end() + 140)
        previous_end = matches[index - 1].end() if index else 0
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        before = normalized[max(start, previous_end):match.start()]
        after = normalized[match.end():min(end, next_start)]
        # Newlines have been joined only between Chinese/digit characters above.
        before = _PUNCTUATION.split(before)[-1]
        after = _PUNCTUATION.split(after)[0]
        after_clause = after.split(",", 1)[0]
        role = "unspecified"
        if re.search(r"(?:發文日期|公告日期|發布日期|發布時間|發佈日期)[:：]?\s*$", before):
            role = "publication"
        elif re.search(r"(?:依|依據|參照|參考)[^。；;]{0,30}$", before) or re.match(r"[^，,。]{0,35}(?:號函|號令)", after):
            role = "reference"
        elif re.search(r"(?:恢復|恢复|終止|停止適用|截止|屆滿|廢止)", after_clause[:90]):
            role = "effective_end"
        elif before.endswith("至") and re.match(r"(?:止|屆滿)", after_clause):
            role = "effective_end"
        elif re.search(r"(?:起實施|起生效|起適用|起施行|生效|開始實施)", after_clause[:100]):
            role = "effective_start"
        elif before.endswith("自") and re.match(r"(?:起至|起|至|實施|施行)", after_clause):
            role = "effective_start"
        elif re.search(r"(?:生效日期|生效日|實施日期|實施日)[:：]?\s*$", before):
            role = "effective_start"
        elif re.search(r"(?:截止日期|截止日|恢復日期|恢復日)[:：]?\s*$", before):
            role = "effective_end"
        boundary = "date_only"
        if role in {"effective_start", "effective_end"}:
            if re.search(r"一般交易時段(?:結束|收盤)後", after_clause):
                boundary = "after_regular_session"
            elif re.search(r"交易時段(?:結束|收盤)後", after_clause):
                boundary = "after_trading_session"
            elif re.search(r"(?:一般交易時段|日盤)(?:開盤|開始)", after_clause):
                boundary = "regular_session_open"
            elif re.search(r"\d{1,2}(?::\d{2}|時(?:\d{1,2}分)?)", after_clause):
                boundary = "explicit_clock"
        result.append({
            "date_iso": value,
            "boundary": boundary,
            "role": role,
            "evidence": _clean(before + match.group() + after),
        })
    return result


def _column_names(cells: Iterable[str]) -> list[str]:
    names: list[str] = []
    for index, cell in enumerate(cells):
        base = _clean(cell) or f"column_{index + 1}"
        name, duplicate = base, 2
        while name in names:
            name = f"{base}__{duplicate}"
            duplicate += 1
        names.append(name)
    return names


def _table(matrix: list[list[str]], index: int, warnings: list[str]) -> dict[str, Any] | None:
    if not matrix:
        return None
    # CSV metadata (e.g. an effective-date line) remains in `text`, never a header.
    header_index = next((
        i for i, row in enumerate(matrix)
        if sum(bool(c.strip()) for c in row) >= 2
        and not re.match(r"(?:最新更新|更新日期|生效日期|單位[:：])", row[0].strip())
    ), 0)
    header = matrix[header_index]
    width = max(map(len, matrix[header_index:]), default=0)
    if width > MAX_TABLE_COLUMNS:
        warnings.append("table_column_limit_reached")
        width = MAX_TABLE_COLUMNS
    names = _column_names((header + [""] * max(0, width - len(header)))[:width])
    body = matrix[header_index + 1:]
    if len(body) > MAX_TABLE_ROWS:
        warnings.append("table_row_limit_reached")
        body = body[:MAX_TABLE_ROWS]
    return {
        "table_index": index,
        "columns": names,
        "rows": [dict(zip(names, (row + [""] * width)[:width])) for row in body],
    }


def _html(text: str, warnings: list[str]) -> tuple[str, list[dict[str, Any]]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(text, "html.parser")
    region = soup.select_one("#content") or soup.select_one("main") or soup.body or soup
    for node in region.select(
        "script,style,noscript,nav,header,footer,input,select,button,aside,"
        ".sidebar,.side_menu,.sideMenu,.breadcrumb,.share,.pageMenu,.leftMenu"
    ):
        node.decompose()
    # TAIFEX also puts real historical announcement bodies inside <form>.
    # Remove controls above, not their entire parent form and document content.
    # Legacy announcements store HTML escaped inside .myContent, then decode with
    # JavaScript. Decode that one presentation layer, never execute its scripts.
    for node in region.select(".myContent,#news_content"):
        decoded = node.get_text()
        if re.search(r"<(?:p|br|a|table|div|span)\b", decoded, re.I):
            replacement = BeautifulSoup(decoded, "html.parser")
            for dangerous in replacement.select("script,style,noscript"):
                dangerous.decompose()
            node.clear()
            node.append(replacement)
    tables: list[dict[str, Any]] = []
    for table_node in region.find_all("table"):
        if table_node.find("table") is not None:
            continue  # Parse nested real tables, not an outer layout table twice.
        matrix: list[list[str]] = []
        has_spans = False
        for row in table_node.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            if cells:
                matrix.append([_clean(cell.get_text(" ", strip=True)) for cell in cells])
                if any(cell.get("rowspan") not in (None, "1") or cell.get("colspan") not in (None, "1") for cell in cells):
                    has_spans = True
                    warnings.append("html_spanned_cells_preserved_without_semantic_alignment")
        if has_spans and matrix:
            # Do not associate a ragged rowspan/colspan body with the wrong
            # financial header. Raw physical rows remain available for a later
            # table-specific parser, including every original header cell.
            matrix.insert(0, [f"column_{i + 1}" for i in range(max(map(len, matrix)))])
        table = _table(matrix, len(tables), warnings)
        if table is not None:
            tables.append(table)
    return region.get_text("\n", strip=True), tables


def _office_xml(archive: zipfile.ZipFile, name: str) -> ElementTree.Element:
    """Read one allowlisted XML member, never extract or follow relationships."""
    info = archive.getinfo(name)
    if info.flag_bits & 1:
        raise ValueError("encrypted_office_xml_not_parsed")
    if info.file_size > MAX_OFFICE_XML_BYTES:
        raise ValueError("office_xml_byte_limit_exceeded")
    with archive.open(info) as handle:
        content = handle.read(MAX_OFFICE_XML_BYTES + 1)
    if len(content) > MAX_OFFICE_XML_BYTES:
        raise ValueError("office_xml_byte_limit_exceeded")
    # ElementTree must not expand source-defined entities. Removing NULs also
    # catches declarations in UTF-16/32 before parsing; no external XML is read.
    declarations = content.replace(b"\x00", b"").upper()
    if b"<!DOCTYPE" in declarations or b"<!ENTITY" in declarations:
        raise ValueError("office_xml_dtd_or_entity_not_allowed")
    return ElementTree.fromstring(content)


def _office_zip(content: bytes, result: dict[str, Any]) -> None:
    """Extract main document content from DOCX/ODT/ODS without office execution.

    Tables retain physical cells under generic headers. Merged or repeated
    cells are never guessed into semantic financial columns or expanded into
    unbounded spreadsheet grids. Original ZIP bytes remain the reconstruction
    authority; macros, relationships, images and arbitrary ZIPs are not opened.
    """
    warnings = result["warnings"]
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
        if len(names) > MAX_ZIP_MEMBERS:
            raise ValueError("office_zip_member_limit_exceeded")
        if len(set(names)) != len(names):
            raise ValueError("office_zip_duplicate_members")
        if "word/document.xml" in names and "[Content_Types].xml" in names:
            result["format"] = "docx"
            document = _office_xml(archive, "word/document.xml")
            if document.tag != _WORD + "document":
                raise ValueError("unexpected_word_document_root")

            def cell_text(node):
                return "".join(
                    child.text or "" if child.tag == _WORD + "t"
                    else "\t" if child.tag == _WORD + "tab" else "\n"
                    for child in node.iter()
                    if child is not node and child.tag in {
                        _WORD + "t", _WORD + "tab", _WORD + "br", _WORD + "cr", _WORD + "p",
                    }
                )

            paragraphs = document.iter(_WORD + "p")
            table_nodes = document.iter(_WORD + "tbl")
            row_tag, cell_tags = _WORD + "tr", {_WORD + "tc"}
        elif "mimetype" in names and "content.xml" in names:
            mime_info = archive.getinfo("mimetype")
            if mime_info.file_size > 128:
                raise ValueError("unexpected_office_mimetype")
            mime = archive.read(mime_info).decode("ascii").strip()
            formats = {
                "application/vnd.oasis.opendocument.text": "odt",
                "application/vnd.oasis.opendocument.spreadsheet": "ods",
            }
            if mime not in formats:
                warnings.append("binary_attachment_preserved_not_parsed")
                return
            result["format"] = formats[mime]
            document = _office_xml(archive, "content.xml")
            if document.tag != _ODF_OFFICE + "document-content":
                raise ValueError("unexpected_odf_document_root")

            def rich_text(node):
                if node.tag == _ODF_TEXT + "s":
                    count = max(1, int(node.get(_ODF_TEXT + "c", "1")))
                    if count > 1024:
                        warnings.append("odf_whitespace_limit_reached")
                    return " " * min(count, 1024)
                if node.tag in {_ODF_TEXT + "tab", _ODF_TEXT + "line-break"}:
                    return "\t" if node.tag == _ODF_TEXT + "tab" else "\n"
                return (node.text or "") + "".join(rich_text(child) + (child.tail or "") for child in node)

            def cell_text(node):
                text = rich_text(node)
                if not text.strip():
                    text = next((node.get(_ODF_OFFICE + key) for key in
                                 ("string-value", "value", "date-value", "time-value", "boolean-value")
                                 if node.get(_ODF_OFFICE + key) is not None), "")
                return text

            paragraphs = (node for node in document.iter() if node.tag in {_ODF_TEXT + "p", _ODF_TEXT + "h"})
            table_nodes = document.iter(_ODF_TABLE + "table")
            row_tag = _ODF_TABLE + "table-row"
            cell_tags = {_ODF_TABLE + "table-cell", _ODF_TABLE + "covered-table-cell"}
            if any(node.get(_ODF_TABLE + key) not in (None, "1") for node in document.iter()
                   for key in ("number-columns-repeated", "number-rows-repeated", "number-columns-spanned", "number-rows-spanned")):
                warnings.append("odf_repeated_or_spanned_cells_preserved_without_expansion")
        else:
            warnings.append("binary_attachment_preserved_not_parsed")
            return
        warnings.append("office_main_content_only_images_relationships_and_macros_not_executed")
        result["text"] = "\n".join(cell_text(node) for node in paragraphs)
        for table_node in table_nodes:
            matrix = []
            # iter() includes ODF header/group wrappers; raw generic cells do
            # not claim a grid alignment or perform spreadsheet calculations.
            for row_node in table_node.iter(row_tag):
                cells = [node for node in row_node if node.tag in cell_tags]
                if cells:
                    matrix.append([_clean(cell_text(node)) for node in cells[:MAX_TABLE_COLUMNS]])
                    if len(cells) > MAX_TABLE_COLUMNS:
                        warnings.append("table_column_limit_reached")
                if len(matrix) >= MAX_TABLE_ROWS:
                    warnings.append("table_row_limit_reached")
                    break
            if matrix:
                warnings.append("office_table_physical_cells_without_semantic_alignment")
                width = max(map(len, matrix))
                table = _table([[f"column_{i + 1}" for i in range(width)]] + matrix,
                               len(result["tables"]), warnings)
                result["tables"].append(table)
        result["parsing_status"] = "parsed" if result["text"].strip() or result["tables"] else "empty"


def extract_document(content: bytes, content_type: str, url: str) -> dict[str, Any]:
    """Extract bounded text/tables and explicit date evidence from public bytes.

    URL and MIME are format hints only. Magic bytes override them, so an HTTP 200
    HTML error returned for a `.csv` URL never becomes a successful CSV table.
    Keep the original bytes/HTTP receipt in the caller regardless of this result.
    """
    result: dict[str, Any] = {
        "parser_version": PARSER_VERSION,
        "format": "unknown", "text": "", "tables": [],
        "parsing_status": "unsupported", "temporal_mentions": [], "warnings": [],
    }
    warnings: list[str] = result["warnings"]
    if len(content) > MAX_DOCUMENT_BYTES:
        result["parsing_status"] = "failed"
        warnings.append("document_byte_limit_exceeded")
        return result
    if not content.strip():
        result["parsing_status"] = "empty"
        return result
    if content.startswith(b"%PDF-"):
        result["format"] = "pdf"
        try:
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(content), strict=False)
            if reader.is_encrypted:
                warnings.append("encrypted_pdf_not_parsed")
                return result
            page_count = len(reader.pages)
            if page_count > MAX_PDF_PAGES:
                warnings.append("pdf_page_limit_reached")
            chunks: list[str] = []
            characters = 0
            for page in reader.pages[:MAX_PDF_PAGES]:
                chunk = page.extract_text() or ""
                chunks.append(chunk)
                characters += len(chunk)
                if characters > MAX_TEXT_CHARACTERS:
                    warnings.append("text_character_limit_reached")
                    break
            result["text"] = "\n\n".join(chunks)[:MAX_TEXT_CHARACTERS]
            warnings.append("pdf_tables_require_layout_validation")
            result["parsing_status"] = "parsed" if result["text"].strip() else "pending_ocr"
        except ImportError:
            warnings.append("pdf_parser_unavailable")
        except Exception as exc:
            # Untrusted PDF failures must not abort the resumable provider batch.
            result["parsing_status"] = "failed"
            warnings.append(f"pdf_parse_error:{type(exc).__name__}")
    elif content.startswith(b"PK\x03\x04"):
        result["format"] = "office_or_archive"
        try:
            _office_zip(content, result)
        except (ValueError, zipfile.BadZipFile, ElementTree.ParseError, KeyError, RuntimeError) as exc:
            result["parsing_status"] = "failed"
            warnings.append(f"office_parse_error:{type(exc).__name__}:{exc}")
    elif content.startswith(b"\xd0\xcf\x11\xe0"):
        result["format"] = "office_or_archive"
        warnings.append("binary_attachment_preserved_not_parsed")
        return result
    else:
        try:
            text = _decode(content)
        except ValueError as exc:
            result["parsing_status"] = "failed"
            warnings.append(str(exc))
            return result
        if re.search(r"<(?:!doctype\s+html|html|body|table|main)\b", text[:2000], re.I):
            result["format"] = "html"
            if re.search(r"<title[^>]*>\s*(?:404|403|Request Rejected)|FOR SECURITY REASONS", text, re.I):
                result["parsing_status"] = "failed"
                warnings.append("html_error_or_access_block")
                return result
            result["text"], result["tables"] = _html(text, warnings)
        else:
            path = unquote(urlsplit(url).path).lower()
            is_csv = "csv" in content_type.lower() or path.endswith((".csv", "down"))
            is_csv = is_csv or ("," in text.splitlines()[0] and "\n" in text)
            if not is_csv:
                result["format"] = "text"
                result["text"] = text[:MAX_TEXT_CHARACTERS]
                warnings.append("unstructured_text_only")
            else:
                result["format"] = "csv"
                try:
                    matrix = []
                    for row in csv.reader(io.StringIO(text, newline=""), strict=True):
                        if any(cell.strip() for cell in row):
                            matrix.append([cell.strip() for cell in row])
                        if len(matrix) > MAX_TABLE_ROWS + 20:
                            warnings.append("table_row_limit_reached")
                            break
                    parsed = _table(matrix, 0, warnings)
                    if parsed and any(
                        # Repeated category headings in the non-equity position
                        # limit CSV are not data under one first-row header.
                        row and row[0] == parsed["columns"][0]
                        for row in matrix[2:]
                    ):
                        warnings.append("csv_repeated_headers_preserved_without_semantic_alignment")
                        width = max(map(len, matrix), default=0)
                        parsed = _table([[f"column_{i + 1}" for i in range(width)]] + matrix, 0, warnings)
                    result["tables"] = [parsed] if parsed else []
                    result["text"] = text
                except csv.Error:
                    result["parsing_status"] = "failed"
                    warnings.append("malformed_csv")
                    return result
        result["parsing_status"] = "parsed" if result["text"].strip() else "empty"
    if len(result["text"]) > MAX_TEXT_CHARACTERS:
        warnings.append("text_character_limit_reached")
        result["text"] = result["text"][:MAX_TEXT_CHARACTERS]
    if any("limit_" in warning for warning in warnings) and result["parsing_status"] == "parsed":
        result["parsing_status"] = "partial"
    result["temporal_mentions"] = temporal_mentions(result["text"])
    result["warnings"] = list(dict.fromkeys(warnings))
    return result


def margin_changes(
    rows: Iterable[Mapping[str, Any]], *, unit_hint: str | None = None,
) -> list[dict[str, Any]]:
    """Normalize explicit before/after margin cells, leaving unknown units unknown.

    ``unit_hint`` may be ``ratio``, ``percent``, or an ISO currency only when a
    caller has independently verified the linked document's unit. The generic
    margintable CSV does not carry a unit and numeric magnitude is not evidence.
    Neither this helper nor extract_document attaches historical effective dates.
    """
    facts: list[dict[str, Any]] = []
    for row_index, source_row in enumerate(rows):
        row = {re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(k))): _clean(v) for k, v in source_row.items()}
        code = row.get("契約代碼", row.get("商品代號", ""))
        name = row.get("契約中文簡稱", row.get("商品名稱", ""))
        if not code:
            continue
        for header, (phase, metric) in _MARGIN_HEADERS.items():
            keys = [key for key in row if key == header or key.startswith(header + "(")]
            for key in keys:
                raw = row[key]
                if not raw or raw in {"-", "--", "NA", "N/A"}:
                    continue
                unit = str(unit_hint or row.get("單位", "")).strip()
                value = unicodedata.normalize("NFKC", raw).replace(",", "").strip()
                if "%" in value or "%" in key or "百分比" in unit or unit == "%":
                    unit = "percent"
                elif any(x in value + key + unit for x in ("新臺幣", "新台幣", "TWD", "NT$")):
                    unit = "TWD"
                elif any(x in value + key + unit for x in ("美元", "USD", "US$")):
                    unit = "USD"
                elif any(x in value + key + unit for x in ("人民幣", "CNY", "RMB")):
                    unit = "CNY"
                elif any(x in value + key + unit for x in ("日圓", "JPY")):
                    unit = "JPY"
                elif unit not in {"ratio", "percent", "TWD", "USD", "CNY", "JPY"}:
                    unit = "unknown"
                number_text = re.sub(r"(?:新臺幣|新台幣|美元|人民幣|日圓|TWD|USD|CNY|JPY|RMB|NT\$|US\$|[%元])", "", value).strip()
                try:
                    number = Decimal(number_text)
                    if not number.is_finite() or number < 0:
                        continue
                except InvalidOperation:
                    continue
                amount_vs_ratio = "ratio" if unit in {"ratio", "percent"} else "amount" if unit != "unknown" else "unknown"
                normalized = number / 100 if unit == "percent" else number
                facts.append({
                    "source_row_index": row_index, "contract_code": code,
                    "contract_name": name, "contract_abc": row.get("契約ABC值", ""),
                    "phase": phase, "margin_kind": metric, "source_column": key,
                    "raw_value": raw, "numeric_value": str(number),
                    "normalized_value": str(normalized) if unit != "unknown" else None,
                    "amount_vs_ratio": amount_vs_ratio,
                    "unit": "fraction" if amount_vs_ratio == "ratio" else unit,
                    "source_unit": unit,
                    "unit_status": "explicit" if unit != "unknown" else "needs_document_unit",
                })
    return facts
