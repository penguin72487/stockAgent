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


BOARD_PDF_ORIGINAL_LAYOUT = 'fed_board_private_pdf_original_v1'
BOARD_PDF_ORIGINAL_QUESTION = 'fed_board_registered_pdf_failure_original_v1'
MAX_PDF_ORIGINAL_BYTES = 32 << 20


def board_pdf_original_source(source):
    """The current registered document scope, not a new PDF/body permission."""
    return bool(source.provider == 'fed_board' and source.allowed and source.request_enabled
        and source.authorization.get('kind') == 'public_domain'
        and source.legacy_release_layout == 'fed_table_release_v10'
        and tuple(source.domains) == ('www.federalreserve.gov',)
        and source.pdf_path_pattern and type(source.pdf_max_response_bytes) is int
        and 0 < source.pdf_max_response_bytes <= MAX_PDF_ORIGINAL_BYTES)


def board_pdf_original_url(url, source):
    if not isinstance(url, str) or not 0 < len(url) <= 8192:
        return False
    parts = urlsplit(url)
    return bool(board_pdf_original_source(source) and parts.scheme == 'https'
        and parts.netloc == 'www.federalreserve.gov' and not parts.query and not parts.fragment
        and re.fullmatch(source.pdf_path_pattern, parts.path))


def private_board_pdf_permit(response, source, requested_url):
    """Keep a failed original without claiming it is scanned, valid or complete.

    Existing registration covers Board document paths. This does not decode a
    PDF, execute its content, infer per-document authorship, admit a body, or
    promote filename/metadata dates to historical publication clocks.
    """
    if (not board_pdf_original_url(requested_url, source) or response.url != requested_url
            or response.redirects
            or response.status != 200
            or response.headers.get('content-type', '').split(';', 1)[0].strip().lower() != 'application/pdf'
            or not isinstance(response.body, bytes)
            or not 0 < len(response.body) <= source.pdf_max_response_bytes
            or not response.body.lstrip().startswith(b'%PDF-')):
        raise NewsError('private_pdf_original_scope_not_verified')
    return {'contract_version': 1, 'question': BOARD_PDF_ORIGINAL_QUESTION, 'provider': source.provider,
        'url': response.url, 'request_url': requested_url, 'document_url': response.url,
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'registered_Board_PDF_private_failure_original_only; no_body_or_authorship_admission'}


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
