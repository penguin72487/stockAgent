#!/usr/bin/env python3
"""Query one ToAlpha research item on demand; never build a bulk mirror.

The issuer, exchange, and fund data in stockAgent keep their own official
collectors. This client is limited to ToAlpha's vendor/editorial overlays and
does not write a training dataset or a persistent response cache.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

import requests


MCP_URL = "https://toalpha.tw/mcp"
KEY_NAME = "TOALPHA_MCP_API_KEY"
PROTOCOL_VERSION = "2025-03-26"
SYMBOL_RE = re.compile(r"^[0-9A-Z]{4,6}$")
ALLOWED_TOOLS = frozenset({
    "data_status", "estimates", "analyst_consensus", "broker_ratings", "top_news",
})


def _load_key(env_file: Path) -> str:
    value = os.environ.get(KEY_NAME)
    if value is None:
        if not env_file.is_file():
            raise ValueError(f"{KEY_NAME} is unset and {env_file} does not exist")
        matches: list[str] = []
        for line in env_file.read_text(encoding="utf-8").splitlines():
            item = line.strip()
            if item.startswith("export "):
                item = item[7:].strip()
            if not item.startswith(KEY_NAME + "="):
                continue
            candidate = item.split("=", 1)[1].strip()
            if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in "\"'":
                candidate = candidate[1:-1]
            matches.append(candidate)
        if len(matches) != 1:
            raise ValueError(f"expected exactly one {KEY_NAME} entry in {env_file}")
        value = matches[0]
    if not value or value.startswith("Bearer ") or any(char.isspace() for char in value):
        raise ValueError(f"{KEY_NAME} must contain only the token, without a Bearer prefix")
    return value


def _arguments(tool: str, symbol: str | None, days: int | None, limit: int | None) -> dict[str, Any]:
    if tool not in ALLOWED_TOOLS:
        raise ValueError(f"unsupported ToAlpha tool: {tool}")
    if tool in {"estimates", "analyst_consensus", "broker_ratings"}:
        if not symbol or not SYMBOL_RE.fullmatch(symbol):
            raise ValueError(f"{tool} requires one 4-6 character stock symbol")
    elif symbol is not None:
        raise ValueError(f"{tool} does not accept --symbol")
    if tool == "broker_ratings":
        if limit is not None:
            raise ValueError("--limit is only valid for top_news")
        if days is not None and not 1 <= days <= 30:
            raise ValueError("broker_ratings --days must be between 1 and 30")
        return {"stock_id": symbol, "days": days or 30}
    if days is not None:
        raise ValueError("--days is only valid for broker_ratings")
    if tool == "top_news":
        if limit is not None and not 1 <= limit <= 10:
            raise ValueError("top_news --limit must be between 1 and 10")
        return {"limit": limit or 10}
    if limit is not None:
        raise ValueError("--limit is only valid for top_news")
    return {"stock_id": symbol} if symbol else {}


def _decode_response(response: requests.Response, expected_id: int | None) -> dict[str, Any] | None:
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "unspecified")
        raise RuntimeError(f"ToAlpha rate limit reached; Retry-After: {retry_after}")
    if response.status_code != 200 and not (expected_id is None and response.status_code == 202):
        raise RuntimeError(f"ToAlpha MCP returned HTTP {response.status_code}")
    if expected_id is None:
        return None
    content_type = response.headers.get("Content-Type", "").lower()
    if "text/event-stream" in content_type:
        events: list[dict[str, Any]] = []
        data_lines: list[str] = []
        # requests defaults to ISO-8859-1 for text/event-stream without an
        # explicit charset. ToAlpha sends UTF-8 Chinese JSON, so decode bytes
        # explicitly before splitting lines; mojibake can create fake breaks.
        try:
            event_text = response.content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError("ToAlpha MCP returned invalid UTF-8 SSE") from exc
        for line in event_text.splitlines() + [""]:
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif not line.strip() and data_lines:
                try:
                    events.append(json.loads("\n".join(data_lines)))
                except json.JSONDecodeError as exc:
                    raise RuntimeError("ToAlpha MCP returned malformed SSE JSON") from exc
                data_lines = []
        replies = [item for item in events if item.get("id") == expected_id]
    else:
        try:
            item = response.json()
        except ValueError as exc:
            raise RuntimeError("ToAlpha MCP returned malformed JSON") from exc
        replies = [item] if isinstance(item, dict) and item.get("id") == expected_id else []
    if len(replies) != 1:
        raise RuntimeError("ToAlpha MCP did not return exactly one matching response")
    reply = replies[0]
    if "error" in reply:
        error = reply["error"]
        code = error.get("code", "unknown") if isinstance(error, dict) else "unknown"
        raise RuntimeError(f"ToAlpha MCP RPC error {code}")
    return reply


def query(tool: str, arguments: dict[str, Any], *, key: str, timeout: float = 25.0) -> dict[str, Any]:
    if tool not in ALLOWED_TOOLS:
        raise ValueError(f"unsupported ToAlpha tool: {tool}")
    if not isinstance(arguments, dict) or arguments != _arguments(
        tool, arguments.get("stock_id"), arguments.get("days"), arguments.get("limit")
    ):
        raise ValueError("ToAlpha request must use the bounded on-demand arguments")
    session = requests.Session()
    headers = {
        "Authorization": "Bearer " + key,
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    session_id: str | None = None

    def post(payload: dict[str, Any], expected_id: int | None) -> dict[str, Any] | None:
        nonlocal session_id
        request_headers = dict(headers)
        if session_id:
            request_headers["Mcp-Session-Id"] = session_id
        try:
            response = session.post(
                MCP_URL, json=payload, headers=request_headers, timeout=timeout,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"ToAlpha MCP transport failed: {type(exc).__name__}") from None
        try:
            if response.headers.get("Mcp-Session-Id"):
                session_id = response.headers["Mcp-Session-Id"]
            return _decode_response(response, expected_id)
        finally:
            response.close()

    try:
        initialized = post({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "stockagent-toalpha-research", "version": "1"},
            },
        }, 1)
        if not isinstance(initialized, dict) or "result" not in initialized:
            raise RuntimeError("ToAlpha MCP initialization failed")
        post({"jsonrpc": "2.0", "method": "notifications/initialized"}, None)
        reply = post({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        }, 2)
        result = reply.get("result") if isinstance(reply, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError("ToAlpha MCP tool result is missing")
        if result.get("isError"):
            raise RuntimeError(f"ToAlpha MCP tool {tool} returned an error")
        content = result.get("content")
        if not isinstance(content, list) or len(content) != 1 or content[0].get("type") != "text":
            raise RuntimeError("ToAlpha MCP tool returned an unexpected content shape")
        try:
            payload = json.loads(content[0]["text"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("ToAlpha MCP tool returned invalid JSON content") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("ToAlpha MCP tool returned non-object JSON content")
        return payload
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool", required=True, choices=sorted(ALLOWED_TOOLS))
    parser.add_argument("--symbol", help="One stock code for estimates, consensus, or broker ratings.")
    parser.add_argument("--days", type=int, help="Broker ratings lookback, at most 30 days.")
    parser.add_argument("--limit", type=int, help="Top news count, at most 10.")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parents[1] / ".env")
    args = parser.parse_args()
    try:
        arguments = _arguments(args.tool, args.symbol, args.days, args.limit)
        key = _load_key(args.env_file)
        payload = query(args.tool, arguments, key=key)
    except (ValueError, RuntimeError) as exc:
        print(f"toalpha query failed: {exc}", file=sys.stderr)
        return 1
    # This is an observation now, not evidence that the value existed at its
    # subject period. Keep it outside canonical training and cold releases.
    result = {
        "source": "ToAlpha MCP",
        "tool": args.tool,
        "arguments": arguments,
        "first_observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        "historical_point_in_time": False,
        "data": payload,
    }
    encoded = json.dumps(result, ensure_ascii=False, indent=2)
    print(encoded.replace(key, "[REDACTED]"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
