from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time

import pyarrow as pa
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, atomic_write_text, sha256_file
from downloader.news_fulltext import accept_discovery, discovery_links, fetch_article, fetch_discovery, load_config, run_cycle, seed_discovery
from downloader.news_fulltext_bulk import wordpress_url
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article, normalize_url, public_address_check
from downloader.news_fulltext_queue import NewsQueue, verify_bodies


@pytest.fixture
def config():
    config = json.loads(Path("configs/news_fulltext.json").read_text())
    config.pop("provider_catalog", None)  # Standalone synthetic publisher fixture.
    config.pop("failure_diagnostic_focus", None)  # This fixture has no real Cnyes URL scope.
    # Standalone fixtures cannot inherit live opt-ins requiring a reviewed
    # Cnyes cohort; keep the paired configuration explicit rather than weaken
    # production validation when its focus is missing.
    config["failure_diagnostic_metadata_identity_upgrade"] = False
    config["retained_cnyes_failure_replay_enabled"] = False
    source = config["providers"][0]
    source.update({"id": "publisher", "domains": ["publisher.test"], "interval_seconds": 0.001,
                   "feeds": [], "sitemaps": [], "wordpress_apis": [], "fulltext_feeds": [], "index_pages": [], "index_path_pattern": "",
                   "article_path_pattern": "", "article_selectors": [".story"],
                   "require_configured_container": False, "require_author_attribution": False, "title_selector": "", "author_selectors": [],
                   "required_license_url_prefix": "", "exclude_selectors": []})
    config["providers"] = [source]
    config["min_free_disk_bytes"] = 1
    config["adaptive_rate"] = {"enabled": False}
    config["exclusive_http_owner"] = False
    return config


class FakeLimiter:
    def __init__(self, interval_seconds, **kwargs):
        self.interval_seconds = interval_seconds
        self.delays = []

    def wait(self, **kwargs):
        pass

    def defer(self, delay):
        self.delays.append(delay)


class FakeResponse:
    def __init__(self, body=b"", status=200, headers=None):
        self.body, self.status_code = body, status
        self.headers = headers or {"Content-Type": "text/html"}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, **kwargs):
        yield self.body


class FakeSession:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **kwargs):
        assert kwargs["allow_redirects"] is False
        assert kwargs["stream"] is True
        self.calls.append((url, kwargs))
        result = self.routes[url]
        if isinstance(result, list):
            return result.pop(0)
        return result

    def close(self):
        pass


def http_fixture(config, tmp_path, routes):
    session = FakeSession(routes)
    registry = PolicyRegistry(config, base=tmp_path)
    http = NewsHTTP(registry, config, tmp_path, session_factory=lambda: session,
                    address_check=lambda url: None, limiter_factory=FakeLimiter)
    return http, session


def article_html(*, body=None, free=True):
    body = body or "這是一篇保留原始內容的新聞報導，不是標題或摘要。" * 16
    data = {"@graph": [{"@type": "WebSite"}, {"@type": "NewsArticle", "headline": "測試新聞",
                        "articleBody": body, "isAccessibleForFree": free,
                        "datePublished": "2014-02-03", "author": {"name": "作者"}}]}
    return ('<html><title>News</title><script type="application/ld+json">' +
            json.dumps(data, ensure_ascii=False) + '</script></html>').encode()


@pytest.mark.parametrize("url,expected", [
    (" HTTPS://News.Publisher.Test/path?id=1&utm_source=x#content ", "https://news.publisher.test/path?id=1"),
    ("https://publisher.test/?symbol=2330&date=2026-10-08&fbclid=a", "https://publisher.test/?symbol=2330&date=2026-10-08"),
    ("https://publisher.test/?q=a%2fb&ref=7", "https://publisher.test/?q=a%2fb&ref=7"),
])
def test_url_identity(url, expected):
    assert normalize_url(url) == expected


@pytest.mark.parametrize("url", [None, "", "file:///etc/passwd", "https://a:b@publisher.test/", "https://publisher.test:8080/",
                                "https://publisher.test\\evil/", "https://publisher.test/\n", "https://[::1]/"])
def test_url_rejects_unsafe_input(url):
    with pytest.raises(NewsError):
        normalize_url(url)


def test_private_dns_is_rejected(monkeypatch):
    monkeypatch.setattr("socket.getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("127.0.0.1", 443))])
    with pytest.raises(NewsError, match="non_public_address"):
        public_address_check("https://publisher.test/")


def test_disabled_or_unknown_has_zero_network(config, tmp_path):
    config["providers"][0]["enabled"] = False
    http, session = http_fixture(config, tmp_path, {})
    for url in ("https://publisher.test/story", "https://another.test/story", "https://publisher.test.evil.test/"):
        with pytest.raises(NewsError, match="awaiting_authorization"):
            http.get(url)
    assert session.calls == []


def test_diagnostic_http_owner_cannot_double_source_pressure(config, tmp_path):
    config["exclusive_http_owner"] = True
    http, session = http_fixture(config, tmp_path, {})
    try:
        with pytest.raises(NewsError, match="news_http_owner_already_running"):
            http_fixture(config, tmp_path, {})
        assert session.calls == []
    finally:
        http.close()
    restored, session = http_fixture(config, tmp_path, {})
    restored.close()
    assert session.calls == []


def test_http_200_challenge_pauses_the_source_without_bypassing_it(config, tmp_path):
    url = "https://publisher.test/story"
    http, session = http_fixture(config, tmp_path, {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        url: FakeResponse(b"<title>Just a moment</title><p>Verify you are human</p>")})
    try:
        with pytest.raises(NewsError, match="access_challenge") as outcome:
            fetch_article(http, {"url": url, "id": b"a" * 32})
        assert outcome.value.retry and outcome.value.delay >= 900
        assert http.provider_not_before["publisher"] > time.time()
        before = len(session.calls)
        with pytest.raises(NewsError, match="provider_cooldown"):
            http.get(url)
        assert len(session.calls) == before
    finally:
        http.close()


def test_enabled_does_not_invent_permission(config, tmp_path):
    config["providers"][0]["authorization"] = {"kind": "awaiting_authorization"}
    registry = PolicyRegistry(config, base=tmp_path)
    assert not registry.sources["publisher"].allowed


def test_expired_permission(config, tmp_path):
    config["providers"][0]["authorization"]["valid_until"] = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    assert not PolicyRegistry(config, base=tmp_path).sources["publisher"].allowed


def test_missing_written_permission_file(config, tmp_path):
    config["providers"][0]["authorization"] = {"kind": "written_authorization", "full_text_download": True,
                                               "evidence_file": "missing.txt", "scope": "text download"}
    assert not PolicyRegistry(config, base=tmp_path).sources["publisher"].allowed


def test_robots_wildcard_longest_allow(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow: /*private*\nAllow: /private/public/\n"),
        "https://publisher.test/private/public/story": FakeResponse(article_html()),
    })
    with pytest.raises(NewsError, match="robots_disallowed"):
        http.get("https://publisher.test/a-private-story")
    assert http.get("https://publisher.test/private/public/story").status == 200
    assert len(session.calls) == 2  # Shared robots cache, one permitted article.


def test_robots_fractional_delay_and_rate(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nCrawl-delay: 2.5\nRequest-rate: 1/5\n"),
    })
    http._robots("https://publisher.test/a")
    assert http.limiters["publisher"].interval_seconds == 5


def test_redirect_cannot_escape_policy(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow: /\nAllow: /a\n"),
        "https://publisher.test/a": FakeResponse(status=302, headers={"Location": "https://evil.test/x"}),
    })
    with pytest.raises(NewsError, match="awaiting_authorization"):
        http.get("https://publisher.test/a")
    assert len(session.calls) == 2


def test_redirect_checks_destination_robots(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow: /secret\n"),
        "https://publisher.test/a": FakeResponse(status=302, headers={"Location": "/secret"}),
    })
    with pytest.raises(NewsError, match="robots_disallowed"):
        http.get("https://publisher.test/a")
    assert len(session.calls) == 2


def test_retry_after_defers_shared_bucket(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(status=429, headers={"Retry-After": "120"}),
    })
    with pytest.raises(NewsError) as caught:
        http.get("https://publisher.test/a")
    assert caught.value.retry and caught.value.delay == 120
    assert http.limiters["publisher"].delays == [120]
    assert http.provider_not_before["publisher"] > time.time()


def test_restart_keeps_publisher_cooldown_without_claiming_other_articles(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {})
    http.defer("publisher", 120)
    http.close()
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/a"}, {"url": "https://publisher.test/b"}])
        status = run_cycle(queue, http, config, max_articles=64, max_discovery_requests=0)
        assert not session.calls
        assert status["states"] == {"pending": 2}
        assert status["provider_next_attempt_utc"]["publisher"]
        assert not status["http_requests_this_run"]
    finally:
        queue.close()
        http.close()


def test_legacy_shared_cooldown_skips_before_lease(config, tmp_path):
    class DeferredLimiter(FakeLimiter):
        def remaining_delay_seconds(self):
            return 600
    registry = PolicyRegistry(config, base=tmp_path)
    session = FakeSession({})
    http = NewsHTTP(registry, config, tmp_path, session_factory=lambda: session,
                    address_check=lambda url: None, limiter_factory=DeferredLimiter)
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/a"}])
        status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert status["states"] == {"pending": 1}
        assert not session.calls
        assert "publisher" in status["provider_next_attempt_utc"]
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize("headers,body", [({"Content-Length": "100"}, b"a"), ({}, b"a" * 100)])
def test_response_size_is_bounded(config, tmp_path, headers, body):
    config["max_response_bytes"] = 50
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(body, headers=headers),
    })
    with pytest.raises(NewsError, match="response_too_large"):
        http.get("https://publisher.test/a")


def test_extract_jsonld_and_source_clock(config, tmp_path):
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    result = extract_article(article_html(), source)
    assert result["published_at_raw"] == "2014-02-03"  # Not invented midnight UTC.
    assert result["extraction_method"] == "jsonld_article_body"
    assert not result["historical_point_in_time"]
    assert "作者" in result["authors_json"]


@pytest.mark.parametrize("html,code", [
    (b"<title>Just a moment...</title><article>blocked</article>", "access_challenge"),
    (b'<article class="paywall">buy</article>', "paywall_or_login"),
    (article_html(free=False), "paywall_or_login"),
    (b'<meta property="og:description" content="summary only"><h1>Headline</h1>', "body_not_found"),
])
def test_teasers_and_access_pages_are_not_bodies(config, tmp_path, html, code):
    with pytest.raises(NewsError, match=code):
        extract_article(html, PolicyRegistry(config, base=tmp_path).sources["publisher"])


def test_css_body_excludes_navigation_and_media(config, tmp_path):
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    payload = ('<h1>標題</h1><div class="story"><nav>導覽</nav><p>' + "保留原文，不下載外部媒體。" * 15 +
               '</p><figure>照片第三方圖說</figure></div>').encode()
    result = extract_article(payload, source)
    assert result["body_completeness"] == "article_container_unverified"
    assert "導覽" not in result["body"] and "照片" not in result["body"]


def metadata_receipt(tmp_path, rows):
    parquet = tmp_path / "metadata.parquet"
    atomic_write_parquet(parquet, pa.Table.from_pylist(rows))
    receipt = tmp_path / "source.json"
    atomic_write_json(receipt, {"sha256": sha256_file(parquet), "rows": len(rows), "parquet_path": "metadata.parquet"})
    return receipt


def test_incremental_dedup_preserves_associations(config, tmp_path):
    receipt = metadata_receipt(tmp_path, [
        {"url": "https://publisher.test/a?utm_source=x", "stock_id": "2330", "date": "2026-10-08"},
        {"url": "https://publisher.test/a", "stock_id": "2317", "date": "2026-10-08"},
        {"url": None, "stock_id": "1101", "date": "2026-10-08"},
    ])
    q = NewsQueue(tmp_path / "queue", PolicyRegistry(config, base=tmp_path))
    try:
        assert q.ingest_receipt(tmp_path, receipt, provider="finlab", link_column="url")
        assert not q.ingest_receipt(tmp_path, receipt, provider="finlab", link_column="url")
        s = q.status()
        assert s["unique_articles"] == 1 and s["metadata_association_rows"] == 3 and s["invalid_metadata_urls"] == 1
        assert q.db.execute("SELECT association_rows FROM source_links").fetchone()[0] == 2
        assert s["all_sources_eta_seconds"] is None and s["eligible_eta_seconds"] is None
    finally:
        q.close()


def test_bad_metadata_does_not_enter_queue(config, tmp_path):
    receipt = metadata_receipt(tmp_path, [{"url": "https://publisher.test/a"}])
    data = json.loads(receipt.read_text())
    data["sha256"] = "0" * 64
    atomic_write_json(receipt, data)
    q = NewsQueue(tmp_path / "queue", PolicyRegistry(config, base=tmp_path))
    try:
        assert not q.ingest_receipt(tmp_path, receipt, provider="finlab", link_column="url")
        assert q.status()["unique_articles"] == 0
        assert q.status()["metadata_partition_errors"] == 1
    finally:
        q.close()


def test_metadata_revisions_not_double_counted_as_current_rows(config, tmp_path):
    receipt = metadata_receipt(tmp_path, [{"url": "https://publisher.test/a"}])
    q = NewsQueue(tmp_path / "queue", PolicyRegistry(config, base=tmp_path))
    try:
        assert q.ingest_receipt(tmp_path, receipt, provider="finlab", link_column="url")
        receipt = metadata_receipt(tmp_path, [{"url": "https://publisher.test/a"}, {"url": "https://publisher.test/b"}])
        assert q.ingest_receipt(tmp_path, receipt, provider="finlab", link_column="url")
        status = q.status()
        assert status["metadata_association_rows"] == 2
        assert status["metadata_partitions"] == 1
        assert status["metadata_partition_versions_retained"] == 2
        assert status["unique_articles"] == 2
    finally:
        q.close()


def test_exclusive_owner_and_restart_lease(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    assert q.claim("publisher", 300)
    with pytest.raises(RuntimeError, match="owner_already_running"):
        NewsQueue(tmp_path, registry)
    q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert q.status()["states"] == {"retry": 1}
        assert q.claim("publisher", 300)
    finally:
        q.close()


def test_durable_receipt_recovers_crash_before_queue_commit(config, tmp_path, monkeypatch):
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    row = q.claim("publisher", 300)
    http, _ = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(article_html()),
    })
    record = fetch_article(http, row)
    monkeypatch.setattr(q, "_accept_records", lambda *a: (_ for _ in ()).throw(RuntimeError("power loss")))
    with pytest.raises(RuntimeError, match="power loss"):
        q.accept([record])
    assert q.status()["states"] == {"inflight": 1}
    q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert q.status()["states"] == {"complete": 1}
        assert q.status()["counters"]["accepted_versions"] == 1
    finally:
        q.close()


def test_retry_has_finite_budget_and_metrics_agree(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([{"url": "https://publisher.test/a"}, {"url": "https://unknown.test/a"}])
        row = q.claim("publisher", 300)
        q.failed(row, NewsError("transport_error", retry=True, delay=1), max_attempts=1)
        assert q.status()["states"] == {"retry_cooldown": 1, "awaiting_authorization": 1}
        actual = {r[0]: r[1] for r in q.db.execute("SELECT state,count(*) FROM articles GROUP BY state")}
        assert q.status()["states"] == actual
        assert not q.claim("publisher", 300)
        with q.db:
            q.db.execute("UPDATE articles SET due=0 WHERE state='retry_cooldown'")
        resumed = q.claim("publisher", 300)
        assert resumed is not None and resumed["attempts"] == 0
        q.failed(resumed, NewsError("source_not_found"), max_attempts=1)
        assert q.status()["states"] == {"source_not_found": 1, "awaiting_authorization": 1}
        assert not q.claim("publisher", 300)
    finally:
        q.close()


@pytest.mark.parametrize('code,retry', [('transport_error', True), ('source_not_found', False),
                                     ('source_article_empty_template', False), ('source_access_denied', True)])
def test_failed_attempt_has_actual_check_clock_without_rewriting_capture(config, tmp_path, monkeypatch, code, retry):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([{'url': 'https://publisher.test/a'}])
            q.db.execute("UPDATE articles SET captured_at_utc='2020-01-01T00:00:00+00:00',body_sha256='retained-proof'")
        row = q.claim('publisher', 300)
        observed = '2026-10-10T01:02:03+00:00'
        monkeypatch.setattr('downloader.news_fulltext_queue.utc_now', lambda: observed)
        q.failed(row, NewsError(code, retry=retry, delay=120), max_attempts=6)
        actual = q.db.execute('SELECT error_code,last_checked_at_utc,captured_at_utc,body_sha256 FROM articles').fetchone()
        assert tuple(actual) == (code, observed, '2020-01-01T00:00:00+00:00', 'retained-proof')
        assert q.status()['article_failure_observation_contract_version'] == 2
    finally:
        q.close()


def test_changed_permission_releases_only_eligible(config, tmp_path):
    disabled = deepcopy(config)
    disabled["providers"][0]["enabled"] = False
    q = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}, {"url": "https://unknown.test/a"}])
    q.close()
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert q.status()["states"] == {"pending": 1, "awaiting_authorization": 1}
    finally:
        q.close()


def test_sitemap_and_feed_are_filtered_not_bodies(config, tmp_path):
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    rss = b'<rss><channel><item><title>T</title><link>https://publisher.test/2026/10/08/a</link></item><item><link>https://evil.test/2026/10/08/b</link></item></channel></rss>'
    assert discovery_links(rss, "feed", source) == [("https://publisher.test/2026/10/08/a", "T", "")]
    index = b'<sitemapindex><sitemap><loc>https://publisher.test/one.xml</loc></sitemap></sitemapindex>'
    assert discovery_links(index, "sitemap", source) == [("https://publisher.test/one.xml", "sitemap", "")]


def test_run_cycle_accepts_body_and_not_a_404(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}, {"url": "https://publisher.test/b"}])
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(article_html()),
        "https://publisher.test/b": FakeResponse(status=404),
    })
    try:
        s = run_cycle(q, http, config, max_articles=2, max_discovery_requests=0)
        assert s["states"] == {"complete": 1, "source_not_found": 1}
        assert s["accepted_text_bytes"] > 0
        assert s["http_requests_this_run"] == {"publisher": 3}
        assert len(list((tmp_path / "receipts").glob("*.json"))) == 1
    finally:
        http.close()
        q.close()


def test_default_configuration_separates_personal_capture_from_licenses():
    config, registry, repo = load_config(Path("configs/news_fulltext.json"))
    expected = {
        "globalvoices", "fed_board", "cnyes", "cmoney", "cmnews", "einfo", "moneyweekly",
        "ftvnews", "nownews", "genetinfo", "pansci", "cmmedia", "abmedia", "peopo", "cnews", "tainan_gov", "kmdn", "macau_gov",
        "voa_chinese", "voa_cantonese", "tfc_taiwan", "nccu", "taichung_gov", "changhua_gov", "taiwanhot", "coolloud", "eventsinfocus"}
    # Closed installed set; FTNN may cross the tested configuration cutover
    # only under the reviewed native/private recipe, not arbitrary enablement.
    ftnn = registry.sources['ftnn']
    if ftnn.allowed:
        assert ftnn.known_url_only and ftnn.acquisition_mode == 'continuous'
        assert ftnn.domains == ('ftnn.com.tw',)
        assert ftnn.embedded_article_layout == 'ftnn_visible_article_v1'
        assert ftnn.authorization['kind'] == 'personal_research'
        assert ftnn.authorization['evidence_url'] == 'https://www.ftnn.com.tw/copyright'
        assert ftnn.selectors == (':scope > .news-body',)
        assert not ftnn.feeds and not ftnn.sitemaps
        expected.add('ftnn')
    # TVBS joins only through its tested three-layout/private native contract.
    tvbs = registry.sources['tvbs']
    if tvbs.allowed:
        assert tvbs.known_url_only and tvbs.acquisition_mode == 'continuous'
        assert tvbs.domains == ('news.tvbs.com.tw', 'cars.tvbs.com.tw', 'supertaste.tvbs.com.tw')
        assert tvbs.embedded_article_layout in {'tvbs_visible_article_v1', 'tvbs_visible_article_v2'}
        assert tvbs.article_path_pattern == '^(?:/[a-z][a-z-]*/[1-9][0-9]*|/exhibition/ai-trends-2026/article_(?:1|3)-3[.]html)$'
        assert tvbs.authorization['kind'] == 'personal_research'
        assert tvbs.authorization['evidence_url'] == 'https://www.tvbs.com.tw/policies/copyright'
        assert tvbs.selectors == (':scope > .tvbs-native-body',)
        assert tvbs.require_configured_container and tvbs.require_author_attribution
        assert not tvbs.feeds and not tvbs.sitemaps and not tvbs.wordpress_apis and not tvbs.fulltext_feeds
        expected.add('tvbs')
    # ETtoday joins only through the tested native, literal known-URL contract.
    ettoday = registry.sources['ettoday']
    if ettoday.allowed:
        from downloader.news_fulltext_ettoday import ETTODAY_BODY_SELECTOR, ETTODAY_PATH_PATTERN
        assert ettoday.known_url_only and ettoday.acquisition_mode == 'continuous'
        assert ettoday.domains == ('ettoday.net',)
        assert ettoday.embedded_article_layout == 'ettoday_visible_article_v1'
        assert ettoday.article_path_pattern == ETTODAY_PATH_PATTERN
        assert ettoday.authorization['kind'] == 'personal_research'
        assert ettoday.authorization['evidence_url'] == 'https://member.ettoday.net/clause_copyright.php7'
        assert ettoday.selectors == (ETTODAY_BODY_SELECTOR,)
        assert ettoday.require_configured_container and ettoday.require_author_attribution
        assert not ettoday.feeds and not ettoday.sitemaps and not ettoday.wordpress_apis and not ettoday.fulltext_feeds
        expected.add('ettoday')
    assert {p.provider for p in registry.sources.values() if p.allowed} == expected
    events = registry.sources['eventsinfocus']
    assert events.acquisition_mode == 'finite'
    assert events.embedded_article_layout == 'eventsinfocus_visible_news_v1'
    assert events.authorization['kind'] == 'personal_research'
    assert not events.feeds and not events.sitemaps
    coolloud = registry.sources['coolloud']
    assert coolloud.acquisition_mode == ('continuous' if coolloud.known_url_only else 'finite')
    if coolloud.known_url_only:
        entry = next(p for p in config['providers'] if p['id'] == 'coolloud')
        assert entry['private_known_url_scope'] == 'publisher_original_open_license_v1'
    assert coolloud.authorization['kind'] == 'open_license'
    assert coolloud.authorization['license'] == 'CC-BY-NC-SA-2.0-TW'
    assert coolloud.embedded_article_layout == 'coolloud_visible_original_v1'
    assert coolloud.interval_seconds == 10
    abmedia = registry.sources['abmedia']
    assert abmedia.acquisition_mode == ('continuous' if abmedia.known_url_only else 'finite')
    peopo = registry.sources['peopo']
    assert peopo.acquisition_mode == ('continuous' if peopo.known_url_only else 'finite')
    assert peopo.authorization['kind'] == 'open_license'
    assert peopo.required_license_url_prefix == 'http://creativecommons.org/licenses/'
    assert not registry.sources['pts'].allowed
    assert registry.sources["cnyes"].authorization["kind"] == "personal_research"
    assert registry.sources["cmoney"].authorization.get("license") is None
    assert registry.sources["tainan_gov"].acquisition_mode == "finite"
    kmdn = registry.sources["kmdn"]
    assert kmdn.acquisition_mode == ("continuous" if kmdn.known_url_only else "finite")
    assert registry.sources["tainan_gov"].authorization["kind"] == "open_license"
    assert registry.sources["macau_gov"].acquisition_mode == "finite"
    assert registry.sources["macau_gov"].authorization["kind"] == "open_license"
    assert registry.sources["tfc_taiwan"].acquisition_mode == "finite"
    assert registry.sources["tfc_taiwan"].authorization["kind"] == "open_license"
    assert registry.sources["tfc_taiwan"].authorization["license"] == "TFC-Publisher-Nonprofit-Unmodified-Attribution"
    assert registry.sources["nccu"].acquisition_mode == "finite"
    assert registry.sources["nccu"].authorization["kind"] == "open_license"
    assert registry.sources["nccu"].authorization["license"] == "CC-BY-NC-ND-2.5-TW"
    for provider in ("taichung_gov", "changhua_gov"):
        assert registry.sources[provider].acquisition_mode == "finite"
        assert registry.sources[provider].authorization["kind"] == "open_license"
        assert not registry.sources[provider].feeds
        assert not registry.sources[provider].sitemaps
    assert registry.sources["taichung_gov"].allow_reviewed_supplements
    assert not registry.sources["changhua_gov"].allow_reviewed_supplements
    for provider in ("voa_chinese", "voa_cantonese"):
        assert registry.sources[provider].acquisition_mode == "finite"
        assert registry.sources[provider].permitted_author_names
    assert registry.sources["voa_chinese"].authorization["kind"] == "public_domain"
    assert registry.sources["voa_cantonese"].authorization["kind"] == "personal_research"
    assert registry.sources["voa_cantonese"].allow_reviewed_supplements
    assert registry.sources["kmdn"].authorization["kind"] == "personal_research"
    assert registry.sources["kmdn"].authorization.get("license") is None
    cnews = registry.sources['cnews']
    assert cnews.acquisition_mode == ('continuous' if cnews.known_url_only else 'finite')
    cmmedia = registry.sources['cmmedia']
    assert cmmedia.acquisition_mode == ('continuous' if cmmedia.known_url_only else 'finite')
    assert registry.sources["cnews"].authorization["kind"] == registry.sources["cmmedia"].authorization["kind"] == "personal_research"
    assert not registry.sources["line_today"].allowed
    assert not config["publish"] and not config["remote_training_export"]


def personal_source(config):
    config["providers"][0]["authorization"] = {
        "kind": "personal_research", "scope": "public text, local personal research only",
        "evidence_url": "https://publisher.test/terms", "reviewed_on": "2026-10-08",
        "automated_access_review": "no_explicit_prohibition_identified",
    }
    return config


@pytest.mark.parametrize("override", [
    {"collection_purpose": "commercial"}, {"publish": True}, {"remote_training_export": True},
])
def test_personal_capture_cannot_enable_other_purposes(config, tmp_path, override):
    personal_source(config)
    config.update(override)
    if config["publish"] or config["remote_training_export"]:
        with pytest.raises(ValueError):
            PolicyRegistry(config, base=tmp_path)
    else:
        assert not PolicyRegistry(config, base=tmp_path).sources["publisher"].allowed


@pytest.mark.parametrize("field,value", [("automated_access_review", "prohibited"), ("reviewed_on", ""), ("evidence_url", "")])
def test_personal_purpose_does_not_override_source_review(config, tmp_path, field, value):
    personal_source(config)["providers"][0]["authorization"][field] = value
    assert not PolicyRegistry(config, base=tmp_path).sources["publisher"].allowed


def test_personal_body_saved_without_inventing_reuse_rights(config, tmp_path):
    personal_source(config)
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow: /private\n"),
        "https://publisher.test/article": FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/article"}])
        row = queue.claim_url("https://publisher.test/article", 300)
        record = fetch_article(http, row)
        assert record["rights_status"] == "not_cleared_for_reuse"
        assert record["collection_purpose"] == "personal_research"
        assert not record["fair_use_legally_determined"] and not record["training_injection_enabled"]
        queue.accept([record])
        assert queue.claim_url(row["url"], 300) is None  # Already saved, no repeated request.
        assert queue.status()["providers"]["publisher"]["rights_status"] == "not_cleared_for_reuse"
        assert verify_bodies(tmp_path)["accepted"]
        with pytest.raises(NewsError, match="robots_disallowed"):
            http.get("https://publisher.test/private/article")
        assert len(session.calls) == 2
    finally:
        queue.close()
        http.close()


def test_selected_article_respects_existing_retry_deadline(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/a"}])
        row = queue.claim_url("https://publisher.test/a", 300)
        queue.failed(row, NewsError("http_429", retry=True, delay=120), 6)
        assert queue.claim_url(row["url"], 300, refresh=True) is None
        assert queue.db.execute("SELECT state FROM articles").fetchone()[0] == "retry"
    finally:
        queue.close()


def test_enqueue_url_file_reuses_frontier_without_scanning_metadata(config, tmp_path, monkeypatch, capsys):
    from scripts.download_news_fulltext import main
    policy = tmp_path / "policy.json"
    links = tmp_path / "urls.txt"
    root = tmp_path / "news"
    atomic_write_json(policy, personal_source(config))
    atomic_write_text(links, "# local selection\nhttps://publisher.test/a?utm_source=x\n\nhttps://publisher.test/a\nhttps://publisher.test/b\n")
    monkeypatch.setattr(NewsQueue, "ingest_local", lambda *a: pytest.fail("selected URLs must not scan the source lake"))
    assert main(["enqueue", "--config", str(policy), "--root", str(root), "--url-file", str(links)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["unique_articles"] == 2
    assert status["selected_urls"]["submitted_url_lines"] == 3
    assert not status["http_requests_this_run"]


def test_fetch_cli_captures_selected_body_once(config, tmp_path, monkeypatch, capsys):
    import scripts.download_news_fulltext as cli
    personal_source(config)
    policy, root, sessions = tmp_path / "policy.json", tmp_path / "news", []
    atomic_write_json(policy, config)
    def transport(registry, cfg, destination):
        http, session = http_fixture(cfg, destination, {
            "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
            "https://publisher.test/a": FakeResponse(article_html()),
        })
        sessions.append(session)
        return http
    monkeypatch.setattr(cli, "NewsHTTP", transport)
    report = tmp_path / 'selected-fetch.json'
    args = ["fetch", "--config", str(policy), "--root", str(root), "--url", "https://publisher.test/a",
            "--output", str(report)]
    assert cli.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["selected_urls"]["captured"] == 1 and json.loads(report.read_text()) == result
    assert cli.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["selected_urls"]["captured"] == 0 and json.loads(report.read_text()) == result
    assert len(sessions[0].calls) == 2 and not sessions[1].calls
    assert verify_bodies(root)["accepted"]


def test_outside_article_path_has_no_network(config, tmp_path):
    config["providers"][0]["article_path_pattern"] = r"^/news/id/[0-9]+$"
    http, session = http_fixture(config, tmp_path, {})
    try:
        with pytest.raises(NewsError, match="unsupported_article_path"):
            fetch_article(http, {"url": "https://publisher.test/account"})
        assert session.calls == []
    finally:
        http.close()


def test_public_redirect_to_homepage_is_not_an_article(config, tmp_path):
    config["providers"][0]["article_path_pattern"] = r"^/news/id/[0-9]+$"
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/news/id/1": FakeResponse(status=302, headers={"Location": "/"}),
        "https://publisher.test/": FakeResponse(article_html()),
    })
    try:
        with pytest.raises(NewsError, match="redirect_not_article"):
            fetch_article(http, {"url": "https://publisher.test/news/id/1"})
    finally:
        http.close()


@pytest.mark.parametrize("name", [None, "", "  ", 0, {"fake": "author"}])
def test_null_or_structured_name_is_not_invented_author(config, tmp_path, name):
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    data = {"@type": "NewsArticle", "articleBody": "Source text " * 40, "headline": "News", "author": {"name": name}}
    payload = ('<script type="application/ld+json">' + json.dumps(data) + '</script>').encode()
    article = extract_article(payload, source)
    assert article["authors_json"] == "[]"
    assert article["attribution_status"] == "source_byline_not_found"


def test_cnyes_container_fallback_excludes_recommendations():
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["cnyes"]
    payload = ('<h1>測試標題</h1><article><main id="article-container"><p>' + '新聞原始正文。' * 30 +
               '</p></main><div>推薦文章' + '不要混入正文。' * 30 + '</div></article>').encode()
    article = extract_article(payload, source)
    assert article["extraction_method"] == "css:#article-container"
    assert "推薦文章" not in article["body"]


def test_globalvoices_main_body_not_related_articles():
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["globalvoices"]
    payload = ('<div class="post-header"><h2 class="post-title">原文標題</h2></div>'
               '<div id="post-header-credit"><div class="contributor-name"><a class="user-link">原作者</a></div></div>'
               '<div class="post-container"><div class="entry"><div class="wp-caption">第三方圖片</div><p>' +
               '這是新聞本文，每一段都要保留，不是推薦文章。' * 20 + '</p></div>'
               '<div class="post-credit-container"><a rel="license" href="https://creativecommons.org/licenses/by/3.0/deed.zh_TW">CC BY</a></div></div>'
               '<article>相關推薦文章' + '不能誤抓這裡的內容。' * 20 + '</article>').encode()
    article = extract_article(payload, source)
    assert article["title"] == "原文標題" and "原作者" in article["authors_json"]
    assert article["extraction_method"] == "css:.post-container .entry"
    assert "相關推薦" not in article["body"] and "第三方圖片" not in article["body"]


def test_globalvoices_requires_article_license_and_attribution():
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["globalvoices"]
    with pytest.raises(NewsError, match="article_license_not_verified"):
        extract_article(article_html(), source)


def test_rss_dates_sort_by_day_not_weekday_and_dispatch_is_indexed(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([{"url": "https://publisher.test/old", "date": "Wed, 1 Oct 2025 18:00:00 GMT"},
                            {"url": "https://publisher.test/new", "date": "Tue, 6 Oct 2026 18:00:00 GMT"}])
        assert q.claim("publisher", 300)["url"].endswith("/new")
        plan = q.db.execute("EXPLAIN QUERY PLAN SELECT * FROM articles INDEXED BY dispatch_latest "
                            "WHERE provider=? AND state=? AND due<=? ORDER BY metadata_order_day DESC LIMIT 1",
                            ("publisher", "pending", time.time())).fetchall()
        assert all("TEMP B-TREE" not in row[3] for row in plan)
    finally:
        q.close()


def test_related_jsonld_is_not_this_article(config, tmp_path):
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    data = [{"@type": "NewsArticle", "url": "https://publisher.test/related", "articleBody": "WRONG " * 100},
            {"@type": "NewsArticle", "url": "https://publisher.test/current", "articleBody": "CURRENT " * 100}]
    html = ('<script type="application/ld+json">' + json.dumps(data) + '</script>').encode()
    body = extract_article(html, source, page_url="https://publisher.test/current")["body"]
    assert "CURRENT" in body and "WRONG" not in body


def test_offline_verification_and_304_preserve_capture_clock(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a", "date": datetime.now(timezone.utc).date().isoformat()}])
    row = q.claim("publisher", 300)
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(article_html()),
    })
    try:
        record = fetch_article(http, row)
        q.accept([record])
        assert verify_bodies(tmp_path)["accepted"]
        with q.db:
            q.db.execute("UPDATE articles SET last_checked_at_utc=?", ((datetime.now(timezone.utc)-timedelta(days=1)).isoformat(),))
        q.schedule_recent_rechecks(3600, 3)
        old = q.claim("publisher", 300)
        q.unchanged(old)
        head = dict(q.db.execute("SELECT * FROM articles").fetchone())
        assert head["captured_at_utc"] == record["captured_at_utc"]
        assert head["last_checked_at_utc"] >= head["captured_at_utc"]
        assert q.status()["articles_with_stored_body"] == 1
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        q.close()
        http.close()


def test_missing_local_body_retries_unconditionally(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    row = q.claim("publisher", 300)
    row.update({"parquet_path": "missing.parquet", "body_sha256": "0" * 64, "etag": "tag"})
    try:
        q.unchanged(row)
        head = dict(q.db.execute("SELECT * FROM articles").fetchone())
        assert head["state"] == "retry" and head["etag"] is None
    finally:
        q.close()


def bulk_posts(count):
    return [{"id": number, "status": "publish", "type": "post", "link": f"https://publisher.test/article-{number}",
             "content": {"protected": False, "rendered": "<p>Full source article text. " + "word " * 40 + "</p>"},
             "title": {"rendered": f"Article {number}"}, "date_gmt": "2026-10-07T01:00:00", "modified_gmt": "2026-10-07T02:00:00",
             "_embedded": {"author": [{"name": "Original author"}]}} for number in range(count)]


def bulk_fixture(config, tmp_path, *, count=100, total=None, posts=None):
    base = "https://publisher.test/wp-json/wp/v2/posts"
    config["providers"][0]["wordpress_apis"] = [base]
    config["providers"][0]["require_author_attribution"] = True
    payload = json.dumps(posts if posts is not None else bulk_posts(count)).encode()
    total = count if total is None else total
    headers = {"Content-Type": "application/json", "X-WP-Total": str(total), "X-WP-TotalPages": str((total + 99) // 100)}
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        wordpress_url(base): FakeResponse(payload, headers=headers),
    })
    q = NewsQueue(tmp_path, http.registry)
    seed_discovery(q)
    task = dict(q.db.execute("SELECT * FROM discovery WHERE kind='wordpress_posts'").fetchone())
    return q, http, session, task, payload


def test_bulk_100_bodies_one_request_and_one_raw_envelope(config, tmp_path):
    q, http, session, task, payload = bulk_fixture(config, tmp_path)
    try:
        response, links, records, errors = fetch_discovery(http, task)
        assert accept_discovery(q, task, response, links, records, errors) == 100
        assert len([url for url, _ in session.calls if "/posts?" in url]) == 1
        assert q.status()["articles_with_stored_body"] == 100
        result = verify_bodies(tmp_path)
        assert result["accepted"] and result["raw_response_bytes"] == len(payload)
        receipt = json.loads(next((tmp_path / "receipts").glob("*.json")).read_text())
        import pyarrow.parquet as pq
        rows = pq.read_table(tmp_path / receipt["parquet_path"]).to_pylist()
        assert sum(row["raw_response_body"] is not None for row in rows) == 1
        assert all(row["source_response_article_id"] == rows[0]["article_id"] for row in rows)
        assert all(not row["per_article_license_verified"] for row in rows)
        assert all(row["published_at_raw"] == "2026-10-07T01:00:00" for row in rows)
        # A repeated page produces no duplicate body versions and no lost clock.
        assert accept_discovery(q, task, response, links, records, errors) == 0
        assert q.status()["counters"]["accepted_versions"] == 100
    finally:
        q.close()
        http.close()


@pytest.mark.parametrize("posts,total", [(bulk_posts(2), 100), ([], 1), (bulk_posts(2)[:1] * 2, 2)])
def test_bulk_truncation_and_duplicate_id_do_not_advance_page(config, tmp_path, posts, total):
    q, http, _, task, _ = bulk_fixture(config, tmp_path, total=total, posts=posts)
    try:
        with pytest.raises(NewsError, match="bulk_pagination_totals_invalid"):
            accept_discovery(q, task, *fetch_discovery(http, task))
        assert q.status()["articles_with_stored_body"] == 0
        assert q.db.execute("SELECT due FROM discovery").fetchone()[0] == 0
    finally:
        q.close()
        http.close()


def test_bulk_quarantines_rights_and_protected_posts_individually(config, tmp_path):
    posts = bulk_posts(4)
    posts[1]["content"]["protected"] = True
    posts[2]["meta"] = {"license": "all-rights-reserved"}
    posts[3]["_embedded"] = {}
    q, http, _, task, _ = bulk_fixture(config, tmp_path, posts=posts, count=4)
    try:
        assert accept_discovery(q, task, *fetch_discovery(http, task)) == 0
        assert q.status()["bulk_item_errors"] == 3
        assert {r[0] for r in q.db.execute("SELECT error_code FROM bulk_item_errors")} == {
            "paywall_or_login", "article_rights_override", "article_attribution_not_verified"}
        assert q.status()["articles_with_stored_body"] == 0
        assert not (tmp_path / "quarantine").exists()
        assert q.status()["states"] == {"pending": 1}
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        q.close()
        http.close()


def test_bulk_receipt_crash_never_advances_page_and_restart_recovers(config, tmp_path, monkeypatch):
    q, http, _, task, _ = bulk_fixture(config, tmp_path, count=2)
    registry = http.registry
    result = fetch_discovery(http, task)
    monkeypatch.setattr(q, "_accept_records", lambda *a: (_ for _ in ()).throw(RuntimeError("power loss")))
    with pytest.raises(RuntimeError, match="power loss"):
        accept_discovery(q, task, *result)
    assert q.db.execute("SELECT due FROM discovery").fetchone()[0] == 0
    q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert verify_bodies(tmp_path)["accepted"]
        assert q.status()["articles_with_stored_body"] == 2
        assert accept_discovery(q, task, *result) == 0
        assert q.db.execute("SELECT due FROM discovery").fetchone()[0] > time.time()
    finally:
        q.close()
        http.close()


def test_bulk_pagination_uses_maximum_documented_page_size(config, tmp_path):
    q, http, _, task, _ = bulk_fixture(config, tmp_path, total=201)
    try:
        assert accept_discovery(q, task, *fetch_discovery(http, task)) == 100
        assert q.status()["bulk_api_observed_articles"] == 201
        assert q.status()["bulk_api_pages_waiting"] == 2
        assert "per_page=100" in wordpress_url("https://publisher.test/wp-json/wp/v2/posts", 3)
        assert [r[0] for r in q.db.execute("SELECT priority FROM discovery ORDER BY priority")] == [1, 2, 3]
    finally:
        q.close()
        http.close()


def test_broken_bulk_adapter_cools_down_without_stopping_html(config, tmp_path):
    posts = bulk_posts(2)
    for post in posts:
        post["_embedded"] = {}
    q, http, session, _, _ = bulk_fixture(config, tmp_path, count=2, posts=posts)
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    session.routes["https://publisher.test/a"] = FakeResponse(article_html())
    try:
        status = run_cycle(q, http, config, max_articles=1, max_discovery_requests=8)
        assert status["articles_with_stored_body"] == 1
        assert status["bulk_item_errors"] == 2
        assert status["bulk_adapter_cooldowns"][0]["error_code"] == "bulk_all_items_unusable"
        assert len([url for url, _ in session.calls if "/posts?" in url]) == 1
        assert len(list((tmp_path / "quarantine").glob("*.parquet"))) == 1
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        q.close()
        http.close()


def test_disabled_bulk_endpoint_never_runs_stale_page_tasks(config, tmp_path):
    q, http, session, _, _ = bulk_fixture(config, tmp_path, count=1)
    disabled = deepcopy(config)
    disabled["providers"][0]["wordpress_apis"] = []
    q.close()
    http.close()
    http, session = http_fixture(disabled, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    try:
        status = run_cycle(q, http, disabled, max_articles=0, max_discovery_requests=8)
        assert session.calls == []
        assert status["articles_with_stored_body"] == 0
    finally:
        q.close()
        http.close()
