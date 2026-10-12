"""Original Board PDF text, bounded and explicit; no OCR/media/active content.

Creation/modification metadata and filename meeting dates are not release times.
Image-only PDFs remain visibly unextracted; no fabricated substitute body.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError


def extract_board_pdf(payload, source, *, page_url):
    path = urlsplit(page_url).path
    if (source.provider != "fed_board" or source.authorization.get("kind") != "public_domain"
            or not source.pdf_path_pattern or not re.fullmatch(source.pdf_path_pattern, path)):
        raise NewsError("unregistered_pdf_document")
    if not payload.lstrip().startswith(b"%PDF-"):
        raise NewsError("invalid_pdf_document")
    try:
        import pymupdf
    except ImportError as error:
        raise NewsError("pdf_text_dependency_unavailable", retry=True, delay=300) from error
    try:
        with pymupdf.open(stream=payload, filetype="pdf") as document:
            if document.needs_pass:
                raise NewsError("paywall_or_login")
            if not 1 <= len(document) <= 2048:
                raise NewsError("pdf_resource_limit")
            parts, size = [], 0
            for page in document:
                text = page.get_text("text", sort=False)
                size += len(text.encode())
                if size > 4 * 1024 * 1024:
                    raise NewsError("pdf_resource_limit")
                parts.append(text)
            body = "\n\n".join(parts).strip()
            if len(body) < 100 or len(body.split()) < 8:
                raise NewsError("pdf_image_only_or_body_not_found")
            metadata = document.metadata or {}
            title = str(metadata.get("title") or PurePosixPath(path).name)
            return {"title": title, "title_basis": "PDF_declared_title" if metadata.get("title") else "source_document_filename_not_inferred_headline",
                    "body": body, "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
                    "authors_json": "[]", "attribution_status": "official_Board_document; individual_byline_not_verified",
                    "published_at_raw": None, "modified_at_raw": None,
                    "published_time_basis": "unknown; PDF_metadata_and_meeting_date_not_release_clock",
                    "declared_canonical_url": page_url, "declared_license_urls_json": "[]",
                    "extraction_method": "fed_pdf_original_text_v1", "body_completeness": "PDF_embedded_text; images_not_OCRed",
                    "pdf_pages": len(document), "pdf_metadata_raw_json": json.dumps(metadata, ensure_ascii=False),
                    "historical_point_in_time": False}
    except NewsError:
        raise
    except (RuntimeError, ValueError) as error:
        raise NewsError("invalid_pdf_document") from error
