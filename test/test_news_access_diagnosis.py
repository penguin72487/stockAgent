from __future__ import annotations

from copy import deepcopy
from contextlib import closing
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


@pytest.fixture
def redirect_queue(config, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/article/1'}])
            queue.db.execute("UPDATE articles SET state='redirect_not_article',error_code='redirect_not_article',"
                             "due=0,last_checked_at_utc=?", (utc_now(),))
        yield queue


@pytest.mark.parametrize('flags', [
    ['--failure-code', 'redirect_not_article'], ['--require-current-missing-body-failure'],
    ['--failure-code', 'redirect_not_article', '--require-current-missing-body-failure'],
])
def test_current_redirect_cli_requires_matching_code_grant_and_no_follow_before_http(tmp_path, monkeypatch, flags):
    from scripts import diagnose_news_access as access
    monkeypatch.setattr(access, 'NewsHTTP', lambda *_a, **_kw: pytest.fail('invalid flags created HTTP owner'))
    with pytest.raises(SystemExit):
        access.main(['--probe', 'https://publisher.test/article/1', '--output', str(tmp_path / 'invalid.json'), *flags])
    assert not (tmp_path / 'invalid.json').exists()


@pytest.mark.parametrize('changes', [
    {'state': 'complete'}, {'body_sha256': 'retained'}, {'due': 1e12}, {'lease_owner': 'other'},
    {'lease_until': 1e12}, {'error_code': 'http_403'}, {'error_code': 'http_429'},
    {'last_checked_at_utc': None},
])
def test_current_redirect_probe_skips_changed_head_without_http_or_requeue(redirect_queue, monkeypatch, changes):
    from scripts import diagnose_news_access as access
    queue = redirect_queue
    with queue.db:
        for key, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {key}=?', (value,))
    previous = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    monkeypatch.setattr(access, 'load_config', lambda _p: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(access, 'NewsHTTP', lambda *_a, **_kw: pytest.fail('stale head created HTTP owner'))
    output = queue.root / 'skipped.json'
    assert access.main(['--probe', previous['url'], '--output', str(output), '--resolve-redirect-only',
        '--require-current-missing-body-failure', '--failure-code', 'redirect_not_article']) == 0
    result = json.loads(output.read_text())
    assert result['state'] == 'skipped_no_longer_due_failed_missing_body' and result['publisher_requests'] == 0
    assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == previous


def test_current_redirect_cli_records_one_no_follow_response_without_body_or_queue_mutation(redirect_queue, monkeypatch):
    from types import SimpleNamespace
    from scripts import diagnose_news_access as access
    queue = redirect_queue
    previous = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    calls, closed = [], []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(url, 302, {'location': 'https://another-publisher.test/article/2'}, b'BODY_MUST_NOT_BE_RETAINED')
    fake = SimpleNamespace(registry=queue.registry, provider_not_before={}, get=get,
        metrics=SimpleNamespace(snapshot=lambda: {}), close=lambda: closed.append(True))
    # Isolate queue storage without pretending that implementation files live
    # in pytest's temporary data directory; provenance hashes the real source.
    repo = Path(access.__file__).resolve().parents[1]
    monkeypatch.setattr(access, 'load_config', lambda _p: (
        {'root': str(queue.root), 'workers': 1}, queue.registry, repo))
    monkeypatch.setattr(access, 'NewsHTTP', lambda *_a, **_kw: fake)
    output = queue.root / 'redirect.json'
    assert access.main(['--probe', previous['url'], '--output', str(output), '--resolve-redirect-only',
        '--require-current-missing-body-failure', '--failure-code', 'redirect_not_article']) == 0
    result = json.loads(output.read_text())
    assert calls == [(previous['url'], {'follow_redirects': False})] and closed == [True]
    assert result['failure_probe_code'] == 'redirect_not_article'
    assert result['current_failed_missing_body_preflight'][0]['last_checked_at_utc'] == previous['last_checked_at_utc']
    item = result['providers'][0]['probes'][0]
    assert item['status'] == 302 and not item['destination_requested'] and not item['article_durably_accepted']
    assert 'BODY_MUST_NOT' not in output.read_text() and not list(queue.root.rglob('*.parquet'))
    assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == previous


def test_access_cli_cannot_change_root_to_duplicate_enabled_publisher_budget(config, tmp_path, monkeypatch):
    from scripts import diagnose_news_access as access
    registry = PolicyRegistry(config, base=tmp_path)
    configurations = iter([({'root': 'isolated', 'workers': 1}, registry, tmp_path),
                           ({'root': 'production'}, registry, tmp_path)])
    monkeypatch.setattr(access, 'load_config', lambda _p: next(configurations))
    monkeypatch.setattr(access, 'NewsHTTP', lambda *_a, **_kw: pytest.fail('overlapping host created HTTP owner'))
    with pytest.raises(ValueError, match='isolated_diagnostic_overlaps_enabled_production_host'):
        access.main(['--probe', 'https://publisher.test/article/1', '--output', str(tmp_path / 'isolated.json')])
    assert not (tmp_path / 'isolated.json').exists()


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
