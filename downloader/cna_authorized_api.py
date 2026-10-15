"""Bounded CNA MCP capability discovery, not a second body downloader.

Use the publisher's documented Bearer/Streamable-HTTP interface. Only MCP
initialization and tool listing are implemented until the real account's
article schema, history and quota are inspected. Never guess a tool or
promote a search result/summary to a full article.
"""
from __future__ import annotations

import codecs
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import time

from dotenv.parser import parse_stream
import httpx

from downloader.news_fulltext_policy import NewsError, public_address_check


CNA_ENDPOINT = "https://ask.cna.com.tw/mcp/connect"
CNA_KEY_NAME = "CNA_API_KEY"
SUPPORTED_PROTOCOLS = frozenset({"2025-03-26", "2025-06-18", "2025-11-25"})
MAX_RESPONSE_BYTES = 1 << 20
MAX_LIST_PAGES = 8
MAX_TOOLS = 512
_SSE_NEWLINE = re.compile(r"\r\n|\r|\n")


def load_cna_key(path: Path) -> str | None:
    """Read one literal secret without shell expansion, mutation or logging."""
    value = os.environ.get(CNA_KEY_NAME)
    if value is None and path.is_file():
        try:
            bindings = list(parse_stream(io.StringIO(path.read_text(encoding="utf-8-sig"))))
        except (OSError, UnicodeError):
            raise NewsError("cna_credentials_unreadable") from None
        values = [b.value for b in bindings if b.key == CNA_KEY_NAME]
        if len(values) > 1:
            raise NewsError("cna_duplicate_credential")
        value = values[0] if values else None
    if value is None or value == "":
        return None
    if (not isinstance(value, str) or len(value) > 4096
            or any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in value)
            or any(marker in value for marker in ("$", "`", "{", "}", "<", ">"))):
        raise NewsError("cna_invalid_credential")
    return value


def _rpc_result(message: object, expected_id: int) -> dict:
    if (not isinstance(message, dict) or message.get("jsonrpc") != "2.0"
            or type(message.get("id")) is not int or message["id"] != expected_id):
        raise NewsError("cna_rpc_identity_mismatch")
    if "error" in message:
        # Error strings can echo credentials or include account information.
        raise NewsError("cna_rpc_error")
    if not isinstance(message.get("result"), dict):
        raise NewsError("cna_rpc_result_missing")
    return message["result"]


def _read_result(response, expected_id: int, deadline: float) -> dict:
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type not in {"application/json", "text/event-stream"}:
        raise NewsError("cna_unexpected_response_type")
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    buffer, lines, size, skip_lf = "", [], 0, False
    try:
        for chunk in response.iter_bytes():
            size += len(chunk)
            if size > MAX_RESPONSE_BYTES:
                raise NewsError("cna_response_too_large")
            if time.monotonic() > deadline:
                raise NewsError("cna_response_deadline")
            buffer += decoder.decode(chunk)
            if content_type == "application/json":
                continue
            # Standard SSE LF/CRLF/CR framing; return as soon as the matching
            # reply arrives, not after an indefinitely open event stream.
            while buffer:
                if skip_lf:
                    if buffer.startswith("\n"):
                        buffer = buffer[1:]
                    skip_lf = False
                match = _SSE_NEWLINE.search(buffer)
                if not match:
                    break
                line, buffer = buffer[:match.start()], buffer[match.end():]
                skip_lf = match.group() == "\r"  # A split CRLF is one newline.
                if line.startswith("data:"):
                    lines.append(line[5:].removeprefix(" "))
                elif not line and lines:
                    data = "\n".join(lines)
                    lines.clear()
                    if not data:  # MCP's optional SSE priming event.
                        continue
                    message = json.loads(data)
                    if not isinstance(message, dict):
                        raise NewsError("cna_invalid_rpc_message")
                    if type(message.get("id")) is int and message["id"] == expected_id:
                        return _rpc_result(message, expected_id)
                    if "method" not in message or "id" in message:
                        raise NewsError("cna_rpc_identity_mismatch")
        buffer += decoder.decode(b"", final=True)
        if content_type == "application/json":
            return _rpc_result(json.loads(buffer), expected_id)
    except (UnicodeError, ValueError, RecursionError):
        raise NewsError("cna_invalid_response_encoding") from None
    raise NewsError("cna_rpc_response_missing")


def discover_cna_capabilities(key: str, *, maximum_pages: int = MAX_LIST_PAGES) -> dict:
    """Authenticated protocol metadata only; no tools/call or article fetch."""
    if type(maximum_pages) is not int or not 1 <= maximum_pages <= MAX_LIST_PAGES:
        raise NewsError("cna_invalid_page_bound")
    if not key or any(c.isspace() or not 33 <= ord(c) <= 126 for c in key) or len(key) > 4096:
        raise NewsError("cna_invalid_credential")
    public_address_check(CNA_ENDPOINT)
    deadline = time.monotonic() + 45
    session_id, protocol, requests_sent = None, None, 0
    headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json",
               "Accept": "application/json, text/event-stream",
               "User-Agent": "StockAgentNewsResearch/1.0 (authorized CNA capability check)"}

    # No proxy environment, auth redirect, automatic HTTP retry or alternate
    # endpoint. An ambiguous POST outcome is not replayed to spend more quota.
    with httpx.Client(follow_redirects=False, trust_env=False, timeout=10) as client:
        def post(method, params=None, request_id=None):
            nonlocal session_id, requests_sent
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise NewsError("cna_response_deadline")
            request_headers = dict(headers)
            if session_id:
                request_headers["MCP-Session-Id"] = session_id
            if protocol:
                request_headers["MCP-Protocol-Version"] = protocol
            message = {"jsonrpc": "2.0", "method": method}
            if params is not None:
                message["params"] = params
            if request_id is not None:
                message["id"] = request_id
            requests_sent += 1
            try:
                with client.stream("POST", CNA_ENDPOINT, json=message, headers=request_headers,
                                   timeout=min(10, remaining)) as response:
                    status = response.status_code
                    if status in {401, 403}:
                        raise NewsError("cna_authentication_or_scope_denied")
                    if status == 429:
                        raise NewsError("cna_rate_limited")
                    if 300 <= status < 400:
                        raise NewsError("cna_auth_redirect_rejected")
                    if request_id is None:
                        if status != 202:
                            raise NewsError("cna_notification_not_accepted")
                        return None
                    if status != 200:
                        raise NewsError("cna_http_error")
                    result = _read_result(response, request_id, deadline)
                    declared_session = response.headers.get("mcp-session-id")
                    if declared_session is not None:
                        if (not 1 <= len(declared_session) <= 512
                                or method != "initialize" and declared_session != session_id
                                or any(not 33 <= ord(c) <= 126 for c in declared_session)):
                            raise NewsError("cna_invalid_session")
                        session_id = declared_session
                    return result
            except httpx.HTTPError:
                raise NewsError("cna_transport_error") from None

        initialized = post("initialize", {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "stockagent-authorized-news-inspector", "version": "1"}}, 1)
        protocol = initialized.get("protocolVersion")
        if protocol not in SUPPORTED_PROTOCOLS:
            raise NewsError("cna_unsupported_protocol")
        if (not isinstance(initialized.get("capabilities"), dict)
                or not isinstance(initialized["capabilities"].get("tools"), dict)):
            raise NewsError("cna_tools_not_supported")
        post("notifications/initialized")
        tools, names, cursors, cursor = [], set(), set(), None
        for page in range(maximum_pages):
            result = post("tools/list", {"cursor": cursor} if cursor is not None else {}, page + 2)
            current = result.get("tools")
            if not isinstance(current, list) or len(tools) + len(current) > MAX_TOOLS:
                raise NewsError("cna_invalid_tool_inventory")
            for item in current:
                if (not isinstance(item, dict) or not isinstance(item.get("name"), str)
                        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", item["name"])
                        or item["name"] in names or not isinstance(item.get("inputSchema"), dict)):
                    raise NewsError("cna_invalid_tool_inventory")
                names.add(item["name"])
                tools.append({field: item[field] for field in
                              ("name", "title", "description", "inputSchema", "outputSchema", "annotations")
                              if field in item})
            cursor = result.get("nextCursor")
            if cursor is None:
                break
            if not isinstance(cursor, str) or not 1 <= len(cursor) <= 4096 or cursor in cursors:
                raise NewsError("cna_invalid_tool_cursor")
            cursors.add(cursor)
        else:
            raise NewsError("cna_tool_page_bound_reached")
    # Private schemas may contain a server echo. Known credentials and session
    # are removed before any caller can publish a receipt or terminal summary.
    def has_secret(value):
        pending = [value]
        while pending:
            current = pending.pop()
            if isinstance(current, str):
                if key in current or session_id and session_id in current:
                    return True
            elif isinstance(current, dict):
                pending.extend(current)
                pending.extend(current.values())
            elif isinstance(current, list):
                pending.extend(current)
        return False

    if has_secret(tools):
        raise NewsError("cna_secret_in_tool_metadata")
    encoded = json.dumps(tools, ensure_ascii=False)
    return {"state": "capabilities_discovered", "connection_verified": True,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(), "protocol_version": protocol,
            "http_requests_sent": requests_sent, "tool_calls_sent": 0, "tool_count": len(tools),
            "tools": tools, "tool_inventory_sha256": hashlib.sha256(encoded.encode()).hexdigest(),
            "full_text_entitlement_verified": False, "quota_verified": False,
            "request_credit_cost_verified": False, "history_start_verified": False,
            "raw_article_receipts_verified": False, "all_history_complete": False}
