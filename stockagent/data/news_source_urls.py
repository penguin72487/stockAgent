"""Resolve *literal* original URLs retained in legacy Google News metadata.

This is a local metadata operation, not a Google client or an access workaround.
The observed legacy envelope is field 1=19, field 4=one literal original URL,
and optionally field 26=an AMP URL/empty value. Google has not documented that
mapping as an API contract. New opaque tokens stay unresolved; no title-based
guess, private RPC, redirect request, signature or cookie is used.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import hashlib
import ipaddress
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, normalize_url
from stockagent.data.news_source_catalog import source_role


GOOGLE_LITERAL_URL_CONTRACT = 1
_PATH = re.compile(r"/(?:rss/|__i/rss/rd/)?articles/([A-Za-z0-9_-]+={0,2})/?\Z")
_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_REDIRECT_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_LITERAL_CONTROL = re.compile(r"[\x00-\x20\x7f]")
_MAX_BYTES = 8192


@dataclass(frozen=True, slots=True)
class OriginalURLResolution:
    outcome: str
    error_code: str = ""
    original_url: str | None = None
    alternate_url: str | None = None  # Evidence only; NEVER a fallback request.
    envelope_sha256: str | None = None
    envelope_bytes: int = 0
    contract_version: int = GOOGLE_LITERAL_URL_CONTRACT


def _varint(payload: bytes, offset: int) -> tuple[int, int]:
    value = 0
    for position in range(10):
        if offset >= len(payload):
            raise ValueError("truncated_varint")
        byte = payload[offset]
        offset += 1
        if position == 9 and byte > 1:
            raise ValueError("overflow_varint")
        value |= (byte & 127) << (position * 7)
        if not byte & 128:
            return value, offset
    raise ValueError("overflow_varint")


def _legacy_fields(payload: bytes) -> dict[int, int | bytes]:
    fields = {}
    offset = 0
    # Three reviewed top-level fields only. No recursive protobuf parsing,
    # repeated-field last-wins ambiguity or scanning binary data for a URL.
    while offset < len(payload):
        tag, offset = _varint(payload, offset)
        field, wire = tag >> 3, tag & 7
        if field not in (1, 4, 26) or wire != (0 if field == 1 else 2):
            raise ValueError("unreviewed_envelope_fields")
        if field in fields:
            raise ValueError("duplicate_envelope_field")
        value, offset = _varint(payload, offset)
        if wire == 0:
            fields[field] = value
        else:
            end = offset + value
            if end > len(payload):
                raise ValueError("truncated_length_field")
            fields[field] = payload[offset:end]
            offset = end
    if fields.get(1) != 19 or not isinstance(fields.get(4), bytes):
        raise ValueError("unreviewed_envelope_fields")
    return fields


def _public_literal(value: bytes) -> str:
    try:
        literal = value.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise ValueError("invalid_url_encoding") from exc
    if literal != literal.strip() or _LITERAL_CONTROL.search(literal):
        raise ValueError("invalid_literal_url")
    try:
        url = normalize_url(literal)
    except NewsError as exc:
        raise ValueError("invalid_literal_url") from exc
    host = urlsplit(url).hostname
    if (not host or len(host) > 253 or "." not in host
            or any(not _LABEL.fullmatch(label) for label in host.split("."))
            or host.endswith((".local", ".localhost", ".internal"))):
        raise ValueError("non_public_literal_host")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        # No literal IP, even a currently public one, is a reviewed publisher.
        raise ValueError("literal_ip_host")
    if source_role(host) == "redirector":
        raise ValueError("original_is_another_redirector")
    return url


def resolve_google_literal_url(url: str) -> OriginalURLResolution:
    """Decode one retained URL, fail closed, and never perform network I/O.

    A resolved URL proves only where metadata pointed. Destination policy,
    robots, publisher identity, author, body and publication time still need
    their existing independent acceptance checks.
    """
    if (not isinstance(url, str) or len(url) > _MAX_BYTES or url != url.strip()
            or _REDIRECT_CONTROL.search(url)):
        return OriginalURLResolution("unresolved", "invalid_redirect_url")
    try:
        parts = urlsplit(url)
        if (parts.scheme not in {"http", "https"} or parts.hostname != "news.google.com"
                or parts.username or parts.password or parts.port not in {None, 80 if parts.scheme == "http" else 443}):
            return OriginalURLResolution("unresolved", "not_google_news_article")
    except ValueError:
        return OriginalURLResolution("unresolved", "invalid_redirect_url")
    match = _PATH.fullmatch(parts.path)
    if not match:
        return OriginalURLResolution("unresolved", "unsupported_google_news_path")
    token = match[1]
    try:
        payload = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        if (not payload or len(payload) > _MAX_BYTES
                or base64.urlsafe_b64encode(payload).decode().rstrip("=") != token.rstrip("=")):
            raise ValueError("noncanonical_base64_envelope")
    except (binascii.Error, ValueError):
        return OriginalURLResolution("unresolved", "invalid_base64_envelope")
    evidence = {"envelope_sha256": hashlib.sha256(payload).hexdigest(), "envelope_bytes": len(payload)}
    try:
        fields = _legacy_fields(payload)
    except ValueError as exc:
        return OriginalURLResolution("unresolved", str(exc), **evidence)
    original = fields[4]
    if not original[:8].lower().startswith((b"http://", b"https://")):
        if original.startswith(b"AU_yqL"):
            return OriginalURLResolution("opaque_original", "opaque_original_url", **evidence)
        return OriginalURLResolution("unresolved", "non_literal_original", **evidence)
    try:
        original_url = _public_literal(original)
        alternate = fields.get(26, b"")
        alternate_url = _public_literal(alternate) if alternate else None
    except ValueError as exc:
        return OriginalURLResolution("unresolved", str(exc), **evidence)
    return OriginalURLResolution("literal_original", original_url=original_url,
                                 alternate_url=alternate_url, **evidence)
