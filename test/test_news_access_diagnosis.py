from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from downloader.news_fulltext_http import NewsHTTP, Response
from downloader.news_fulltext_policy import PolicyRegistry
from scripts.diagnose_news_access import AccessTrace, diagnose_provider
from test_news_fulltext import FakeLimiter, FakeResponse, FakeSession, article_html


@pytest.fixture
def config():
    value = json.loads(Path("configs/news_fulltext.json").read_text())
    value.pop("provider_catalog", None)
    source = deepcopy(value["providers"][1])
    source.update(id="publisher", domains=["publisher.test"], interval_seconds=.001,
                  feeds=[], sitemaps=[], index_pages=[], index_path_pattern="",
                  article_path_pattern="^/article/[0-9]+$", legacy_release_layout="",
                  pdf_path_pattern="", pdf_max_response_bytes=0, html_parser="lxml",
                  require_configured_container=False, title_selector="", author_selectors=[])
    value.update(providers=[source], exclusive_http_owner=False, adaptive_rate={"enabled": False})
    return value


def fixture_http(config, tmp_path, routes):
    registry = PolicyRegistry(config, base=tmp_path)
    trace = AccessTrace(registry)
    session = FakeSession(routes)
    http = NewsHTTP(registry, config, tmp_path, session_factory=lambda: session,
                    address_check=lambda url: None, limiter_factory=FakeLimiter,
                    access_observer=trace.observe)
    return http, trace, session


def test_refusal_headers_are_observed_without_reading_denial_body(config, tmp_path):
    url = "https://publisher.test/article/1"
    class Refusal(FakeResponse):
        def iter_content(self, **kwargs):
            raise AssertionError("denial body must not be read")
    http, trace, session = fixture_http(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /", headers={"Content-Type": "text/plain"}),
        url: Refusal(b"denial", status=403, headers={"Server": "CloudFront", "X-Cache": "Error from cloudfront",
            "Set-Cookie": "private", "Authorization": "secret", "Retry-After": "1000"}),
    })
    try:
        result = diagnose_provider(http, trace, [url])
        row = result["http_path"][-1]
        assert row["status"] == 403 and row["body_read"] is False
        assert row["headers"] == {"server": "CloudFront", "x-cache": "Error from cloudfront", "retry-after": "1000"}
        assert result["probes"][0]["error"] == "source_access_denied"
        assert result["probes"][0]["article_durably_accepted"] is False
        assert http.provider_not_before["publisher"] >= time.time() + 990
        assert len(session.calls) == 2
        files=list((tmp_path/'access_problems'/'publisher').glob('*.json'))
        assert len(files)==1
        saved=json.loads(files[0].read_text())
        assert saved['error_code']=='source_access_denied' and saved['body_read'] is False
        assert saved['headers']==row['headers']
        assert 'Set-Cookie' not in files[0].read_text() and 'secret' not in files[0].read_text()
    finally:
        http.close()


def test_robots_home_redirect_is_identified_but_not_silently_allowed(config, tmp_path):
    url = "https://publisher.test/article/1"
    home = b'<html><head><title>Publisher</title><link rel="canonical" href="https://publisher.test/"></head><body><a href="/article/1">News</a><a href="http://[invalid">Invalid</a></body></html>'
    http, trace, session = fixture_http(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(status=302, headers={"Location": "/"}),
        "https://publisher.test/": FakeResponse(home),
    })
    try:
        result = diagnose_provider(http, trace, [url])
        assert result["probes"][0]["error"] == "robots_not_plain_rules"
        rows = result["http_path"]
        assert [(r["requested_url"],r["status"]) for r in rows] == [("https://publisher.test/robots.txt",302),("https://publisher.test/",200)]
        assert rows[-1]["robots_directive_lines"] == []
        assert rows[-1]["html_structure"]["known_article_links"] == 1
        assert rows[-1]["html_structure"]["malformed_links"] == 1
        assert rows[-1]["html_structure"]["challenge_title"] is False
        assert http.blocked_robots_origins('publisher')['https://publisher.test'] > time.time()
        assert not http.provider_not_before
        assert len(session.calls) == 2  # No article HTTP, fabricated rules or alternate origin.
        assert "raw_response_body" not in json.dumps(result)
    finally:
        http.close()


def test_persisted_cooldown_has_zero_requests(config, tmp_path):
    http, trace, session = fixture_http(config, tmp_path, {})
    try:
        http.defer("publisher", 30)
        result = diagnose_provider(http, trace, ["https://publisher.test/article/1"])
        assert result["probes"][0]["error"] == "persisted_provider_cooldown"
        assert result["probes"][0]["sent"] is False
        assert result["http_path"] == [] and session.calls == []
    finally:
        http.close()


def test_public_link_diagnosis_never_follows_unknown_destination(config, tmp_path):
    url = "https://publisher.test/article/1"
    target = "https://another-publisher.test/article/42"
    http, trace, session = fixture_http(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        url: FakeResponse(status=302, headers={"Location": target, "Set-Cookie": "private"}),
    })
    try:
        result = diagnose_provider(http, trace, [url], resolve_redirect_only=True)
        item = result["probes"][0]
        assert item["status"] == 302 and item["redirect_location"] == target
        assert item["destination_requested"] is False
        assert item["redirected_article_identity_verified"] is False
        assert item["article_durably_accepted"] is False
        assert "parser_result" not in item
        assert len(session.calls) == 2
        assert "private" not in json.dumps(result)
    finally:
        http.close()


def test_redirect_only_does_not_ignore_robots_or_refusal(config, tmp_path):
    url = "https://publisher.test/article/1"
    http, trace, session = fixture_http(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow: /article/"),
    })
    try:
        result = diagnose_provider(http, trace, [url], resolve_redirect_only=True)
        assert result["probes"][0]["error"] == "robots_disallowed"
        assert len(session.calls) == 1
    finally:
        http.close()


def test_redirect_only_leaves_default_cross_provider_guard_in_place(config, tmp_path):
    url = "https://publisher.test/article/1"
    http, trace, session = fixture_http(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        url: FakeResponse(status=302, headers={"Location": "https://another-publisher.test/article/2"}),
    })
    try:
        result = diagnose_provider(http, trace, [url])
        assert result["probes"][0]["error"] == "awaiting_authorization"
        assert len(session.calls) == 2
    finally:
        http.close()


@pytest.mark.parametrize("value", [None, 0, 1, "false"])
def test_follow_redirects_requires_explicit_boolean(config, tmp_path, value):
    http, trace, session = fixture_http(config, tmp_path, {})
    try:
        with pytest.raises(ValueError, match="boolean"):
            http.get("https://publisher.test/article/1", follow_redirects=value)
        assert session.calls == []
    finally:
        http.close()


def test_actual_missing_robots_and_body_are_distinct(config, tmp_path):
    url = "https://publisher.test/article/1"
    http, trace, session = fixture_http(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(status=404), url: FakeResponse(article_html()),
    })
    try:
        result = diagnose_provider(http, trace, [url])
        assert result["http_path"][0]["status"] == 404
        assert result["probes"][0]["parser_result"]["body_sha256"]
        assert result["probes"][0]["article_durably_accepted"] is False
        assert len(session.calls) == 2
    finally:
        http.close()


def test_trace_is_bounded_and_skips_cookie_header(tmp_path, config):
    trace = AccessTrace(PolicyRegistry(config, base=tmp_path))
    for i in range(40):
        response = Response(f"https://publisher.test/article/{i}", 200,
            {"set-cookie": "secret", "server": "test"}, b"private body")
        trace.observe("headers",response)
        trace.observe("complete",response)
    rows = trace.snapshot("publisher")
    assert len(rows) == 32 and rows[0]["requested_url"].endswith("/8")
    assert all(r["headers"] == {"server": "test"} for r in rows)
    assert "private body" not in json.dumps(rows)


def test_trace_rejects_unpaired_result(tmp_path, config):
    trace = AccessTrace(PolicyRegistry(config, base=tmp_path))
    with pytest.raises(ValueError,match="unpaired"):
        trace.observe("complete",Response("https://publisher.test/",200,{},b""))


def test_failed_diagnostic_write_preserves_original_source_failure(config,tmp_path,monkeypatch):
    url='https://publisher.test/article/1'
    http,trace,session=fixture_http(config,tmp_path,{
        'https://publisher.test/robots.txt':FakeResponse(b'User-agent: *\nAllow: /'),
        url:FakeResponse(status=403),
    })
    original=__import__('downloader.news_fulltext_http',fromlist=['atomic_write_json']).atomic_write_json
    def write(path,*args,**kwargs):
        if 'access_problems' in Path(path).parts:
            raise OSError('diagnostic disk unavailable')
        return original(path,*args,**kwargs)
    monkeypatch.setattr('downloader.news_fulltext_http.atomic_write_json',write)
    try:
        result=diagnose_provider(http,trace,[url])
        assert result['probes'][0]['error']=='source_access_denied'
        assert http.metrics.snapshot()['providers']['publisher']['counts']['access_diagnostic_write_errors']==1
        assert http.provider_not_before['publisher']>time.time()
    finally:
        http.close()
