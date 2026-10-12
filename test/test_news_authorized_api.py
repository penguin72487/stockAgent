from __future__ import annotations

import fcntl
import json
from pathlib import Path

import httpx
import pytest

from downloader import cna_authorized_api as api
from downloader.news_fulltext_policy import NewsError
from scripts import inspect_cna_authorized_api as script
from scripts.plan_news_sources import plan_sources


class Response:
    def __init__(self, payload=None, *, status=200, headers=None, chunks=None):
        self.status_code = status
        self.headers = {"content-type": "application/json", **(headers or {})}
        self.chunks = chunks if chunks is not None else [json.dumps(payload, ensure_ascii=False).encode()]

    def iter_bytes(self):
        yield from self.chunks

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def reply(result, request_id=2):
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def tool(name="read_article", **extras):
    # Deliberately a fixture name, never an asserted CNA tool or live schema.
    return {"name": name, "inputSchema": {"type": "object", "properties": {}}, **extras}


def install_client(monkeypatch, results):
    calls = []
    monkeypatch.setattr(api, "public_address_check", lambda url: None)
    init = Response(reply({"protocolVersion": "2025-06-18", "capabilities": {"tools": {}}}, 1),
                    headers={"mcp-session-id": "private-session"})
    responses = iter([init, Response(status=202), *results])

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["follow_redirects"] is False and kwargs["trust_env"] is False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, method, url, **kwargs):
            assert method == "POST" and url == api.CNA_ENDPOINT
            calls.append(kwargs)
            return next(responses)

    monkeypatch.setattr(api.httpx, "Client", Client)
    return calls


def test_literal_cna_key_no_shell_or_global_mutation(tmp_path, monkeypatch):
    monkeypatch.delenv(api.CNA_KEY_NAME, raising=False)
    env = tmp_path / ".env"
    env.write_text("UNRELATED=$(not_executed)\nexport CNA_API_KEY='private-key' # comment\n")
    assert api.load_cna_key(env) == "private-key"
    monkeypatch.setenv(api.CNA_KEY_NAME, "process-key")
    assert api.load_cna_key(env) == "process-key"
    monkeypatch.delenv(api.CNA_KEY_NAME)
    env.write_text("CNA_API_KEY=one\nCNA_API_KEY=two\n")
    with pytest.raises(NewsError, match="duplicate_credential"):
        api.load_cna_key(env)


@pytest.mark.parametrize("value", ["Bearer bad", "$(not_executed)", "${OTHER_KEY}", "bad key", "bad\r\nkey"])
def test_invalid_cna_key_does_not_expose_or_interpolate(tmp_path, monkeypatch, value):
    monkeypatch.setenv(api.CNA_KEY_NAME, value)
    with pytest.raises(NewsError, match="cna_invalid_credential") as error:
        api.load_cna_key(tmp_path / ".env")
    assert value not in str(error.value)


def test_paged_capabilities_preserve_protocol_scope_and_hide_secrets(monkeypatch):
    calls = install_client(monkeypatch, [
        Response(reply({"tools": [tool()], "nextCursor": "page-2"})),
        Response(reply({"tools": [tool("search_fixture")]}, 3), headers={"mcp-session-id": "private-session"}),
    ])
    report = api.discover_cna_capabilities("private-key")
    assert report["connection_verified"] and report["tool_count"] == 2
    assert not report["full_text_entitlement_verified"] and not report["quota_verified"]
    assert report["http_requests_sent"] == 4 and report["tool_calls_sent"] == 0
    assert [c["json"]["method"] for c in calls] == [
        "initialize", "notifications/initialized", "tools/list", "tools/list"]
    assert calls[0]["headers"]["Authorization"] == "Bearer private-key"
    assert "MCP-Session-Id" not in calls[0]["headers"]
    assert all(c["headers"]["MCP-Protocol-Version"] == "2025-06-18" for c in calls[1:])
    assert calls[-1]["json"]["params"] == {"cursor": "page-2"}
    encoded = json.dumps(report)
    assert "private-key" not in encoded and "private-session" not in encoded


@pytest.mark.parametrize("newline", [b"\n", b"\r\n", b"\r"])
def test_sse_utf8_split_and_return_before_infinite_stream(newline):
    payload = json.dumps(reply({"tools": [tool(description="中文測試")]}, 2), ensure_ascii=False).encode()
    wire = b": heartbeat" + newline + b"data: " + newline + newline + b"data: " + payload + newline + newline

    def chunks():
        for part in wire:
            yield bytes([part])
        raise AssertionError("must not wait for SSE to end after a valid response")

    response = Response(headers={"content-type": "text/event-stream"}, chunks=chunks())
    assert api._read_result(response, 2, float("inf"))["tools"][0]["description"] == "中文測試"


@pytest.mark.parametrize("payload,code", [
    ({"jsonrpc": "2.0", "id": 9, "result": {}}, "identity_mismatch"),
    ({"jsonrpc": "2.0", "id": True, "result": {}}, "identity_mismatch"),
    ({"jsonrpc": "2.0", "id": 2, "error": {"message": "secret-key"}}, "rpc_error"),
    ({"jsonrpc": "2.0", "id": 2, "result": []}, "result_missing"),
])
def test_rpc_errors_never_echo_source_message(payload, code):
    with pytest.raises(NewsError, match=code) as error:
        api._read_result(Response(payload), 2, float("inf"))
    assert "secret-key" not in str(error.value)


def test_stream_size_deadline_encoding_and_html_errors():
    cases = [
        (Response(chunks=[b"x" * (api.MAX_RESPONSE_BYTES + 1)]), float("inf"), "too_large"),
        (Response(reply({})), 0, "deadline"),
        (Response(chunks=[b"\xff"]), float("inf"), "encoding"),
        (Response(headers={"content-type": "text/html"}), float("inf"), "response_type"),
    ]
    for response, deadline, code in cases:
        with pytest.raises(NewsError, match=code):
            api._read_result(response, 2, deadline)


@pytest.mark.parametrize("status,code", [(301, "redirect_rejected"), (401, "scope_denied"),
                                          (403, "scope_denied"), (429, "rate_limited"), (503, "http_error")])
def test_source_denials_not_replayed_or_alternate_endpoint(monkeypatch, status, code):
    calls = install_client(monkeypatch, [Response(status=status)])
    with pytest.raises(NewsError, match=code):
        api.discover_cna_capabilities("private-key")
    assert len(calls) == 3 and all(c["json"]["method"] != "tools/call" for c in calls)


def test_cursor_loops_page_bounds_duplicate_tools_and_secret_echo(monkeypatch):
    fixtures = [
        ([Response(reply({"tools": [], "nextCursor": "same"})),
          Response(reply({"tools": [], "nextCursor": "same"}, 3))], 8, "invalid_tool_cursor"),
        ([Response(reply({"tools": [], "nextCursor": "more"}))], 1, "page_bound_reached"),
        ([Response(reply({"tools": [tool(), tool()]}))], 8, "invalid_tool_inventory"),
        ([Response(reply({"tools": [tool(description='account private-key')]}))], 8, "secret_in_tool_metadata"),
        ([Response(reply({"tools": [tool(inputSchema={"properties": {"private-session": {}}})]}))], 8,
         "secret_in_tool_metadata"),
    ]
    for results, pages, code in fixtures:
        install_client(monkeypatch, results)
        with pytest.raises(NewsError, match=code):
            api.discover_cna_capabilities("private-key", maximum_pages=pages)


def test_missing_and_local_only_setup_make_no_network_calls(tmp_path, monkeypatch):
    monkeypatch.delenv(api.CNA_KEY_NAME, raising=False)
    config = Path("configs/news/cna_authorized_api.json")
    monkeypatch.setattr(script, "discover_cna_capabilities", lambda *a, **k: pytest.fail("unexpected source call"))
    result = script.inspect_setup(config, tmp_path / ".env", connect=True, http_root=tmp_path / "queue")
    assert result["state"] == "credential_missing" and result["http_requests_sent"] == 0
    assert not (tmp_path / "queue").exists()
    monkeypatch.setenv(api.CNA_KEY_NAME, "private-key")
    result = script.inspect_setup(config, tmp_path / ".env")
    assert result["state"] == "local_configured_not_connected" and not result["connection_verified"]


def test_capability_probe_obeys_existing_news_http_owner(tmp_path, monkeypatch):
    monkeypatch.setenv(api.CNA_KEY_NAME, "private-key")
    with (tmp_path / "http-owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(NewsError, match="owner_already_running"):
            script.inspect_setup(Path("configs/news/cna_authorized_api.json"), tmp_path / ".env",
                                 connect=True, http_root=tmp_path)


def test_unsupported_or_active_body_config_rejected(tmp_path):
    config = json.loads(Path("configs/news/cna_authorized_api.json").read_text())
    for field, value in (("endpoint", "https://example.com/mcp"), ("article_tool_calls_enabled", True)):
        path = tmp_path / "config.json"
        path.write_text(json.dumps({**config, field: value}))
        with pytest.raises(NewsError, match="unreviewed_capability_config"):
            script.inspect_setup(path, tmp_path / ".env")


def test_cna_authorized_setup_not_reasked_or_misclassified_as_public_html():
    review = json.loads(Path("configs/news/source_reviews_20261009.json").read_text())["providers"]["cna"]
    result = plan_sources({"providers": {"cna": {"source_role": "publisher_or_other_unverified", "known_urls": 90}},
                           "observed_at_utc": "2026-10-09T00:00:00Z"}, reviews={"cna": review})
    row = result["sources"][0]
    assert result["contract_version"] == 5
    assert row["implementation_action"] == "configure_and_verify_authorized_api_contract"
    assert row["collector_family"] == "authorized_provider_api"
    assert row["strategy"] == "authorized_api_configuration_and_capability_check"
    assert "existing user authorization accepted" in row["required_before_requests"]
    assert not result["plan_is_acquisition_acceptance"] and row["finish_eta_utc"] is None
