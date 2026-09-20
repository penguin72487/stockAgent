from __future__ import annotations

import json

import pytest

from scripts import query_toalpha_unique as client


def test_unique_queries_reject_full_market_and_long_lookbacks() -> None:
    assert client._arguments("estimates", "2330", None, None) == {"stock_id": "2330"}
    assert client._arguments("broker_ratings", "2330", 30, None) == {
        "stock_id": "2330", "days": 30,
    }
    assert client._arguments("top_news", None, None, 10) == {"limit": 10}
    with pytest.raises(ValueError, match="requires one"):
        client._arguments("broker_ratings", None, 30, None)
    with pytest.raises(ValueError, match="between 1 and 30"):
        client._arguments("broker_ratings", "2330", 31, None)
    with pytest.raises(ValueError, match="unsupported"):
        client._arguments("monthly_revenue", "2330", None, None)
    with pytest.raises(ValueError, match="requires one"):
        client.query("broker_ratings", {"days": 30}, key="example-token")


def test_key_is_loaded_without_sourcing_other_env_entries(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv(client.KEY_NAME, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("UNRELATED=$(touch /tmp/should-never-exist)\nTOALPHA_MCP_API_KEY='example-token'\n")
    assert client._load_key(env_file) == "example-token"
    env_file.write_text("TOALPHA_MCP_API_KEY=first\nTOALPHA_MCP_API_KEY=second\n")
    with pytest.raises(ValueError, match="exactly one"):
        client._load_key(env_file)


def test_query_uses_bearer_header_and_rejects_redirects(monkeypatch) -> None:
    class Response:
        def __init__(self, status: int, payload: dict | None):
            self.status_code = status
            self.headers = {"Content-Type": "application/json", "Mcp-Session-Id": "session-1"}
            self._payload = payload

        def json(self):
            return self._payload

        def close(self):
            pass

    class Session:
        def __init__(self):
            self.calls = []

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            method = kwargs["json"]["method"]
            if method == "initialize":
                return Response(200, {"jsonrpc": "2.0", "id": 1, "result": {}})
            if method == "notifications/initialized":
                return Response(202, None)
            return Response(200, {
                "jsonrpc": "2.0", "id": 2,
                "result": {"content": [{"type": "text", "text": json.dumps({"as_of": "2026-09-16"})}]},
            })

        def close(self):
            pass

    session = Session()
    monkeypatch.setattr(client.requests, "Session", lambda: session)
    assert client.query("estimates", {"stock_id": "2330"}, key="hidden-token") == {
        "as_of": "2026-09-16"
    }
    assert [call[1]["json"]["method"] for call in session.calls] == [
        "initialize", "notifications/initialized", "tools/call",
    ]
    assert all(call[0] == client.MCP_URL for call in session.calls)
    assert all(call[1]["allow_redirects"] is False for call in session.calls)
    assert all(call[1]["headers"]["Authorization"] == "Bearer hidden-token" for call in session.calls)


def test_sse_response_requires_matching_rpc_id() -> None:
    class Response:
        status_code = 200
        headers = {"Content-Type": "text/event-stream"}
        content = 'event: message\ndata: {"jsonrpc":"2.0","id":2,"result":{"read":"台股"}}\n\n'.encode()

    assert client._decode_response(Response(), 2)["result"]["read"] == "台股"
    with pytest.raises(RuntimeError, match="matching response"):
        client._decode_response(Response(), 3)
