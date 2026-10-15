"""Representation negotiation fixes; no new endpoint, identity or retries."""
from __future__ import annotations

from copy import deepcopy
import json

import pytest

from downloader.news_fulltext import accept_discovery, discovery_fingerprint, fetch_discovery, seed_discovery
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, config as config, http_fixture
from test_news_metadata_feed_scope import config as metadata_config, setup as metadata_setup, task as metadata_task, xml
from test_news_provider_efficiency import feed_item, rss


DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml,text/plain;q=0.9"
FEED_MEDIA = {"application/rss+xml", "application/atom+xml", "text/xml"}


def media_types(header):
    return {part.split(";", 1)[0].strip() for part in header.split(",")}


@pytest.mark.parametrize("family,path,query", [
    ("feeds", "/feeds/news.xml", ""),
    ("feeds", "/feeds/news.xml", "?cursor=2"),
    ("fulltext_feeds", "/feed/", ""),
    ("fulltext_feeds", "/feed/", "?paged=2"),
])
def test_registered_feed_negotiates_supported_types_without_changing_identity(config, tmp_path, family, path, query):
    base = "https://publisher.test" + path
    config["providers"][0][family] = [base]
    url = base + query
    http, session = http_fixture(config, tmp_path, {url: FakeResponse(b"original feed")})
    try:
        assert http._raw(url).body == b"original feed"
        assert len(session.calls) == 1 and session.calls[0][0] == url
        headers = session.calls[0][1]["headers"]
        assert media_types(headers["Accept"]) == FEED_MEDIA | media_types(DEFAULT_ACCEPT)
        assert "*/*" not in headers["Accept"] and headers["User-Agent"] == config["user_agent"]
        assert http.requests_by_provider == {"publisher": 1}
    finally:
        http.close()


@pytest.mark.parametrize("path", ["/robots.txt", "/other.xml", "/unregistered/feed/", "/story/1", "/other.pdf"])
def test_unregistered_or_nonfeed_endpoint_keeps_original_accept(config, tmp_path, path):
    config["providers"][0]["feeds"] = ["https://publisher.test/feeds/news.xml"]
    url = "https://publisher.test" + path
    http, session = http_fixture(config, tmp_path, {url: FakeResponse(b"original")})
    try:
        assert http._raw(url).body == b"original"
        assert session.calls[0][1]["headers"]["Accept"] == DEFAULT_ACCEPT
        assert len(session.calls) == 1
    finally:
        http.close()


@pytest.mark.parametrize("representation", ["application/rss+xml", "application/atom+xml", "text/xml"])
@pytest.mark.parametrize("declare_original_only", [False, True])
def test_standard_negotiation_requires_exact_supported_media_without_extra_calls(config, tmp_path, monkeypatch,
                                                                              representation, declare_original_only):
    url = "https://publisher.test/feeds/news.xml"
    config["providers"][0]["feeds"] = [url]
    http, session = http_fixture(config, tmp_path, {url: FakeResponse()})
    original_get = session.get

    def negotiate(target, **kwargs):
        original_get(target, **kwargs)
        accepted = representation in media_types(kwargs["headers"]["Accept"])
        return FakeResponse(b"original response" if accepted else b"not acceptable", status=200 if accepted else 406,
                            headers={"Content-Type": representation})

    monkeypatch.setattr(session, "get", negotiate)
    try:
        result = http._raw(url, {"Accept": DEFAULT_ACCEPT} if declare_original_only else None)
        assert result.status == (406 if declare_original_only else 200)
        assert result.body == (b"not acceptable" if declare_original_only else b"original response")
        assert len(session.calls) == 1 and http.requests_by_provider == {"publisher": 1}
    finally:
        http.close()


def test_caller_headers_and_registered_pdf_negotiation_keep_precedence(config, tmp_path):
    source = config["providers"][0]
    url = "https://www.federalreserve.gov/feeds/test.xml"
    pdf = "https://www.federalreserve.gov/monetarypolicy/files/FOMCoriginal.pdf"
    source.update(id="fed_board", domains=["federalreserve.gov"], feeds=[url],
                  authorization={"kind": "public_domain", "license": "US-government-public-domain", "scope": "Board text",
                                 "evidence_url": "https://www.federalreserve.gov/disclaimer.htm"},
                  pdf_path_pattern=r"^/monetarypolicy/files/FOMC[A-Za-z0-9_.-]+[.]pdf$", pdf_max_response_bytes=1024)
    http, session = http_fixture(config, tmp_path, {url: FakeResponse(), pdf: FakeResponse(b"%PDF-original")})
    try:
        assert http._raw(url, {"Accept": "application/atom+xml", "If-None-Match": '"original"'}).status == 200
        assert session.calls[0][1]["headers"]["Accept"] == "application/atom+xml"
        assert session.calls[0][1]["headers"]["If-None-Match"] == '"original"'
        assert http._raw(pdf).body == b"%PDF-original"
        assert session.calls[1][1]["headers"]["Accept"] == "application/pdf"
        assert len(session.calls) == 2
    finally:
        http.close()


@pytest.mark.parametrize("status,code", [(403, "source_access_denied"), (429, "http_429")])
def test_feed_refusal_keeps_original_cooldown_and_no_immediate_replay(config, tmp_path, status, code):
    url = "https://publisher.test/feed/"
    config["providers"][0]["fulltext_feeds"] = [url]
    http, session = http_fixture(config, tmp_path, {url: FakeResponse(b"DO NOT READ", status=status,
                                            headers={"Retry-After": "1200", "Content-Type": "application/rss+xml"})})
    try:
        with pytest.raises(NewsError, match=code) as refused:
            http._raw(url)
        assert refused.value.delay >= 1200
        with pytest.raises(NewsError, match="provider_cooldown"):
            http._raw(url)
        assert len(session.calls) == 1 and http.response_bytes == 0
        assert http.requests_by_provider == {"publisher": 1}
    finally:
        http.close()


def test_conditional_feed_304_retains_validators_and_original_fingerprint(config, tmp_path):
    url = "https://publisher.test/feeds/news.xml"
    config["providers"][0]["feeds"] = [url]
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /", headers={"Content-Type": "text/plain"}),
        url: FakeResponse(status=304, headers={"Content-Type": "application/rss+xml"}),
    })
    try:
        source = http.registry.sources["publisher"]
        task = {"url": url, "provider": "publisher", "kind": "feed", "etag": '"original"',
                "last_modified": "Thu, 08 Oct 2026 17:20:00 GMT", "parser_fingerprint": discovery_fingerprint(source)}
        response, links, records, errors = fetch_discovery(http, task)
        assert response.status == 304 and links == records == errors == []
        assert len(session.calls) == 2
        headers = session.calls[1][1]["headers"]
        assert headers["If-None-Match"] == task["etag"] and headers["If-Modified-Since"] == task["last_modified"]
        assert FEED_MEDIA <= media_types(headers["Accept"])
        assert session.calls[0][1]["headers"]["Accept"] == DEFAULT_ACCEPT
    finally:
        http.close()


def test_one_negotiated_fulltext_feed_preserves_two_original_bodies_and_receipts(config, tmp_path):
    url = "https://publisher.test/feed/"
    config["providers"][0].update(fulltext_feeds=[url], require_author_attribution=True)
    payload = rss([feed_item(1), feed_item(2)])
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /", headers={"Content-Type": "text/plain"}),
        url: FakeResponse(payload, headers={"Content-Type": "application/rss+xml"}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE url=?", (url,)).fetchone())
        response, links, records, errors = fetch_discovery(http, task)
        assert response.body == payload and not errors and len(records) == 2
        assert all(row["published_at_raw"] == "Thu, 08 Oct 2026 13:20:00 -0400" for row in records)
        assert all(json.loads(row["authors_json"]) == ["Real Byline"] for row in records)
        assert accept_discovery(queue, task, response, links, records, errors) == 2
        assert verify_bodies(tmp_path)["accepted"]
        assert len(session.calls) == 2 and FEED_MEDIA <= media_types(session.calls[1][1]["headers"]["Accept"])
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 2
    finally:
        queue.close()
        http.close()


def test_metadata_feed_negotiation_does_not_grant_html_or_fullbody_permission(metadata_config, tmp_path):
    url = "https://news.ltn.com.tw/rss/all.xml"
    queue, http, session = metadata_setup(deepcopy(metadata_config), tmp_path, {
        "https://news.ltn.com.tw/robots.txt": FakeResponse(b"User-agent: *\nAllow: /", headers={"Content-Type": "text/plain"}),
        url: FakeResponse(xml(), headers={"Content-Type": "application/rss+xml"}),
    })
    try:
        task = metadata_task(queue)
        response, links, records, errors = fetch_discovery(http, task)
        assert not http.registry.sources["ltn"].allowed
        assert accept_discovery(queue, task, response, links, records, errors) == 0
        assert FEED_MEDIA <= media_types(session.calls[-1][1]["headers"]["Accept"])
        assert len(session.calls) == 2
        assert queue.db.execute("SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL").fetchone()[0] == 0
        with pytest.raises(NewsError):
            http.get("https://news.ltn.com.tw/news/business/breakingnews/123")
        assert len(session.calls) == 2
    finally:
        queue.close()
        http.close()
