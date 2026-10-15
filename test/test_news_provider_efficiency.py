from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time
from datetime import datetime, timezone

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import accept_discovery, discovery_links, fetch_discovery, load_config, run_cycle, run_service, seed_discovery
from downloader.news_fulltext_feed import feed_page, feed_records, feed_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config as config, http_fixture


def rss(items):
    return ('<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>' + ''.join(items) + '</channel></rss>').encode()


def feed_item(number=1, *, author="Real Byline", fulltext=True, rights=""):
    return (f'<item><title>Story {number}</title><link>https://publisher.test/story/{number}</link>'
            '<pubDate>Thu, 08 Oct 2026 13:20:00 -0400</pubDate>'
            f'<dc:creator>{author}</dc:creator><description>Only a summary</description>' +
            ('<content:encoded><![CDATA[<p>' + 'Publisher original text with several words. ' * 12 + '</p>]]></content:encoded>' if fulltext else '') +
            (f'<dc:rights>{rights}</dc:rights>' if rights else '') + '</item>')


def feed_fixture(config, tmp_path):
    config["providers"][0]["fulltext_feeds"] = ["https://publisher.test/feed/"]
    config["providers"][0]["require_author_attribution"] = True
    http, _ = http_fixture(config, tmp_path, {})
    return http, NewsQueue(tmp_path, http.registry)


def test_fulltext_feed_durable_batch_preserves_raw_clock_and_no_invented_total(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        response = Response(task["url"], 200, {"content-type": "application/rss+xml"}, rss([feed_item(1), feed_item(2)]))
        records, errors = feed_records(response, http, http.registry.sources["publisher"])
        assert not errors and len(records) == 2
        assert records[0]["published_at_raw"].endswith("-0400")
        assert json.loads(records[0]["authors_json"]) == ["Real Byline"]
        assert accept_discovery(queue, task, response, [(r["requested_url"], r["title"], r["metadata_date_raw"]) for r in records], records, errors) == 2
        assert queue.db.execute("SELECT count(*) FROM bulk_api_coverage").fetchone()[0] == 0
        assert queue.db.execute("SELECT count(*) FROM discovery WHERE url LIKE '%paged=2'").fetchone()[0] == 1
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize("item,reason", [(feed_item(fulltext=False), "body_not_found"),
                                         (feed_item(author=""), "article_attribution_not_verified"),
                                         (feed_item(rights="All rights reserved"), "article_rights_override")])
def test_feed_never_promotes_summary_missing_credit_or_override(config, tmp_path, item, reason):
    http, queue = feed_fixture(config, tmp_path)
    try:
        response = Response("https://publisher.test/feed/", 200, {}, rss([item]))
        records, errors = feed_records(response, http, http.registry.sources["publisher"])
        assert not records and errors[0]["error"] == reason
    finally:
        queue.close()
        http.close()


def test_mixed_feed_rights_refuses_shared_raw_and_keeps_metadata(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        response = Response(task["url"], 200, {}, rss([feed_item(1), feed_item(2, rights="All rights reserved")]))
        records, errors = feed_records(response, http, http.registry.sources["publisher"])
        assert accept_discovery(queue, task, response,
            [(r["requested_url"], r["title"], r["metadata_date_raw"]) for r in records], records, errors) == 0
        assert queue.db.execute("SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL").fetchone()[0] == 0
        assert not list(tmp_path.glob("**/*.parquet"))
        assert queue.db.execute("SELECT count(*) FROM articles").fetchone()[0] == 1
        assert queue.db.execute("SELECT 1 FROM discovery WHERE url=?", (feed_url(task['url'], 2),)).fetchone()
        assert queue.db.execute("SELECT error_code FROM discovery WHERE url=?", (task['url'],)).fetchone()[0] == 'bulk_response_contains_restricted_text'
    finally:
        queue.close()
        http.close()


def test_xml_controls_in_body_recover_only_metadata_and_continue_discovery(config, tmp_path):
    from downloader.news_fulltext_feed import XML_BODY_METADATA_FALLBACK
    http, queue = feed_fixture(config, tmp_path)
    url = feed_url('https://publisher.test/feed/', 12)
    payload = rss([feed_item(1).replace('Publisher original', 'Publisher\x08 original'), feed_item(2)])
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'rss_fulltext','publisher')", (url,))
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        response = Response(url, 200, {'content-type': 'application/rss+xml'}, payload)
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        assert not records and [e['error'] for e in errors] == [XML_BODY_METADATA_FALLBACK] * 2
        links = [(e['url'], e['title'], e['metadata_date_raw']) for e in errors]
        assert all(date == 'Thu, 08 Oct 2026 13:20:00 -0400' for _, _, date in links)
        assert accept_discovery(queue, task, response, links, records, errors) == 0
        assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='pending'").fetchone()[0] == 2
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == XML_BODY_METADATA_FALLBACK
        assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?', (feed_url('https://publisher.test/feed/', 13),)).fetchone()
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == 2
        assert not list(tmp_path.glob('**/*.parquet'))  # Neither changed text nor unreviewed shared raw retained.
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        accept_discovery(queue, task, Response(url, 304, {}, b''), [], [], [])
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == XML_BODY_METADATA_FALLBACK
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == 2
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('location', ['link', 'title', 'pubDate', 'structure', 'doctype', 'broken-tag'])
def test_xml_metadata_recovery_does_not_fix_identity_clock_or_other_structure(config, tmp_path, location):
    from downloader.news_fulltext_feed import feed_document
    payload = rss([feed_item()])
    if location in {'link', 'title', 'pubDate'}:
        payload = payload.replace(('<' + location + '>').encode(), ('<' + location + '>\x08').encode())
    elif location == 'structure':
        payload = payload.replace(b'<item>', b'<ite\x08m>')
    else:
        payload = payload.replace(b'Publisher original', b'Publisher\x08 original')
        payload = (b'<!DOCTYPE rss []>' + payload if location == 'doctype' else payload.replace(b'</item>', b'</bad-item>'))
    with pytest.raises(NewsError, match='invalid_feed'):
        feed_document(payload)
    assert not list(tmp_path.glob('**/*.parquet'))


def test_independent_xml_parser_contract_requeue_is_once_and_keeps_source_refusals(config, tmp_path):
    config['providers'][0]['fulltext_feeds'] = ['https://publisher.test/feed/']
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    source_fingerprint = registry.sources['publisher'].data_fingerprint
    url = feed_url('https://publisher.test/feed/', 12)
    refused = feed_url('https://publisher.test/feed/', 13)
    foreign = feed_url('https://publisher.test/undeclared-feed/', 14)
    try:
        # Simulate the previous owner's discovery ABI; source/body policy did
        # not change. A first fresh start without failures already made a marker.
        with queue.db:
            queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'rss_xml_controls_parser_recheck:%'")
            for job, error in ((url, 'invalid_feed'), (refused, 'source_access_denied'), (foreign, 'invalid_feed')):
                queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag) "
                                 "VALUES (?,'rss_fulltext','publisher',? ,?,2,'old')", (job, time.time() + 86400, error))
        queue.reconcile_policy()
        assert queue.db.execute('SELECT due,attempts,error_code,etag FROM discovery WHERE url=?', (url,)).fetchone()[:] == (0, 2, 'invalid_feed', None)
        assert registry.sources['publisher'].data_fingerprint == source_fingerprint
        for job in (refused, foreign):
            assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (job,)).fetchone()[0] > time.time()
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (time.time() + 1000, url))
        queue.reconcile_policy()
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (url,)).fetchone()[0] > time.time()
    finally:
        queue.close()


@pytest.mark.parametrize('paragraph', [
    '<p>His column is behind a paywall but he can be followed on Instagram.</p>',
    '<p>The report quotes isAccessibleForFree=false as an example, not source metadata.</p>',
    '<p>See the discussion of <a href="https://outside.test/paywall">paywalls</a>.</p>',
])
def test_public_feed_prose_about_paywalls_remains_real_body(config, tmp_path, paragraph):
    http, queue = feed_fixture(config, tmp_path)
    try:
        item = feed_item().replace('</p>]]>', '</p>' + paragraph + ']]>')
        records, errors = feed_records(Response('https://publisher.test/feed/', 200, {}, rss([item])),
                                      http, http.registry.sources['publisher'])
        assert not errors and len(records) == 1
        assert 'paywall' in records[0]['body'] or 'isAccessibleForFree=false' in records[0]['body']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('access_markup', [
    '<div class="paywall">Subscribe</div>', '<div data-paywall="true">Subscribe</div>',
    '<form action="/login">Member sign-in</form>',
    '<meta itemprop="isAccessibleForFree" content="false">',
    '<script type="application/ld+json">{"@type":"NewsArticle","isAccessibleForFree":false}</script>',
    '<script type="application/ld+json">{"@type":"NewsArticle","isAccessibleForFree":"False"}</script>',
    '<p>All rights reserved.</p>',
])
def test_feed_actual_access_controls_and_rights_remain_refused(config, tmp_path, access_markup):
    http, queue = feed_fixture(config, tmp_path)
    try:
        item = feed_item().replace('</p>]]>', '</p>' + access_markup + ']]>')
        records, errors = feed_records(Response('https://publisher.test/feed/', 200, {}, rss([item])),
                                      http, http.registry.sources['publisher'])
        assert not records and errors[0]['error'] == 'article_rights_override'
        assert not list(tmp_path.glob('**/*.parquet'))
    finally:
        queue.close()
        http.close()


def test_changed_feed_parser_requeues_only_failed_registered_page_once(config, tmp_path):
    from dataclasses import replace
    config['providers'][0]['fulltext_feeds'] = ['https://publisher.test/feed/']
    registry = PolicyRegistry(config, base=tmp_path)
    registry.sources['publisher'] = replace(registry.sources['publisher'], data_fingerprint='a' * 64)
    queue = NewsQueue(tmp_path, registry)
    future = time.time() + 86400
    jobs = [
        ('https://publisher.test/feed/?paged=33', 'rss_fulltext', 'bulk_response_contains_restricted_text'),
        ('https://publisher.test/feed/?paged=34', 'rss_fulltext', 'source_rate_limited'),
        ('https://publisher.test/feed/?paged=35', 'rss_fulltext', None),
        ('https://publisher.test/foreign-feed/?paged=36', 'rss_fulltext', 'bulk_response_contains_restricted_text'),
    ]
    with queue.db:
        queue.db.executemany('INSERT INTO discovery(url,kind,provider,due,attempts,error_code,etag) VALUES (?,?,\'publisher\',?,4,?,\'old\')',
                             [(url, kind, future, error) for url, kind, error in jobs])
    queue.close()
    registry.sources['publisher'] = replace(registry.sources['publisher'], data_fingerprint='b' * 64)
    queue = NewsQueue(tmp_path, registry)
    try:
        rows = list(queue.db.execute('SELECT url,due,error_code,attempts,etag FROM discovery ORDER BY url'))
        repaired = next(r for r in rows if r['url'] == jobs[0][0])
        assert repaired['due'] == 0 and repaired['etag'] is None and repaired['attempts'] == 4
        assert repaired['error_code'] == 'bulk_response_contains_restricted_text'
        assert all(r['due'] == future and r['etag'] == 'old' for r in rows if r['url'] != jobs[0][0])
        audit = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('rss_parser_recheck:publisher:' + 'b' * 64,)).fetchone()[0])
        assert len(audit['jobs']) == 1 and audit['jobs'][0]['previous_deadline'] == future
        assert not audit['http_refusals_requeued']
        # Re-running reconciliation/restarting under the same policy must not
        # erase a new source outcome or its next due time.
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (future, jobs[0][0]))
        queue.reconcile_policy()
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (jobs[0][0],)).fetchone()[0] == future
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, registry)
    try:
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (jobs[0][0],)).fetchone()[0] == future
    finally:
        queue.close()


def test_successful_rss_parse_clears_old_error_projection_but_304_preserves_it(config, tmp_path):
    from downloader.news_fulltext import discovery_fingerprint
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute('SELECT * FROM discovery').fetchone())
        with queue.db:
            queue.db.execute('INSERT INTO bulk_item_errors VALUES (?,0,\'publisher\',?,\'article_rights_override\',\'2026-01-01T00:00:00+00:00\')',
                             (task['url'], 'https://publisher.test/story/1'))
        response = Response(task['url'], 304, {}, b'')
        task['parser_fingerprint'] = discovery_fingerprint(http.registry.sources['publisher'])
        accept_discovery(queue, task, response, [], [], [])
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == 1
        response = Response(task['url'], 200, {}, rss([feed_item()]))
        records, errors = feed_records(response, http, http.registry.sources['publisher'])
        accept_discovery(queue, task, response, [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records], records, errors)
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == 0
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('success', [False, True])
def test_old_rss_projection_is_archived_only_after_current_exact_recovery(config, tmp_path, success):
    from downloader.news_fulltext import discovery_fingerprint
    http, queue = feed_fixture(config, tmp_path)
    source = http.registry.sources['publisher']
    url = 'https://publisher.test/feed/?paged=33'
    marker = 'rss_parser_recheck:publisher:' + source.data_fingerprint
    try:
        with queue.db:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'jobs': [{'url': url}], 'observed_at_utc': '2026-10-09T10:00:00+00:00'})))
            queue.db.execute('INSERT INTO discovery(url,kind,provider,last_checked_at_utc,parser_fingerprint,response_sha256,error_code) '
                'VALUES (?,\'rss_fulltext\',\'publisher\',\'2026-10-09T11:00:00+00:00\',?, ?, ?)',
                (url, discovery_fingerprint(source), 'c' * 64, None if success else 'invalid_feed'))
            queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,\'publisher\',?,\'article_rights_override\',?)', [
                (url, 1, 'https://publisher.test/story/1', '2026-10-09T09:00:00+00:00'),
                (url, 2, 'https://publisher.test/story/2', '2026-10-09T11:00:00+00:00')])
        queue.reconcile_successful_feed_rechecks()
        rows = queue.db.execute('SELECT item_number FROM bulk_item_errors ORDER BY item_number').fetchall()
        assert [r[0] for r in rows] == ([2] if success else [1, 2])
        audits = [json.loads(r[0]) for r in queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE \'rss_resolved_item_errors:%\'')]
        assert len(audits) == int(success)
        if success:
            assert audits[0]['retired_projection'][0]['item_number'] == 1
            assert not audits[0]['article_or_source_outcomes_reset']
        changes = queue.db.total_changes
        queue.reconcile_successful_feed_rechecks()
        assert queue.db.total_changes == changes
    finally:
        queue.close()
        http.close()


def test_repeated_feed_page_cannot_claim_history_progress(config, tmp_path):
    http, queue = feed_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        payload = rss([feed_item()])
        for page in (1, 2):
            task["url"] = feed_url("https://publisher.test/feed/", page)
            response = Response(task["url"], 200, {}, payload)
            records, errors = feed_records(response, http, http.registry.sources["publisher"])
            if page == 1:
                accept_discovery(queue, task, response, [(r["requested_url"], r["title"], r["metadata_date_raw"]) for r in records], records, errors)
            else:
                with pytest.raises(NewsError, match="bulk_pagination_no_progress"):
                    accept_discovery(queue, task, response, [], records, errors)
        assert queue.db.execute("SELECT count(*) FROM accepted_batches").fetchone()[0] == 1
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize("url", ["https://publisher.test/feed/?paged=0", "https://publisher.test/feed/?paged=100001",
                                 "https://publisher.test/feed/?paged=x", "https://publisher.test/feed/?p=10",
                                 "https://publisher.test/feed/?paged=1&paged=2"])
def test_feed_page_bounds(url):
    with pytest.raises(NewsError):
        feed_page(url)


def test_configured_index_follows_only_real_approved_links(config, tmp_path):
    config["providers"][0].update(index_path_pattern=r"^/archive/[0-9]{4}[.]htm$", article_path_pattern=r"^/story/[0-9]+$")
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    payload = b'<a href="/archive/1996.htm">1996</a><a href="/story/1">Real article</a><a href="/privacy">privacy</a><a href="https://other.test/story/2">foreign</a>'
    assert discovery_links(payload, "index", source, page_url="https://publisher.test/archive.htm") == [
        ("https://publisher.test/archive/1996.htm", "publisher_index", ""), ("https://publisher.test/story/1", "Real article", "")]
    with pytest.raises(NewsError, match="publisher_index_no_matching_links"):
        discovery_links(b'<a href="/privacy">privacy</a>', "index", source, page_url="https://publisher.test/archive.htm")


def test_provider_reservations_reject_underprovisioned_global_workers(config, tmp_path):
    config["providers"][0]["parallelism"] = 2
    config["workers"] = 1
    config["require_provider_worker_reservations"] = True
    path = tmp_path / "config.json"
    atomic_write_json(path, config)
    with pytest.raises(ValueError, match="cannot reserve"):
        load_config(path, repo=tmp_path)


def test_rolling_budget_does_not_drain_fast_provider_behind_slow_provider(config, tmp_path):
    config["workers"] = 2
    slow_source = deepcopy(config["providers"][0])
    slow_source.update(id="slow", domains=["slow.test"], parallelism=1)
    config["providers"].append(slow_source)
    finished, sent_fast = [], []

    class DelayedResponse(FakeResponse):
        def iter_content(self, **kwargs):
            time.sleep(0.35)
            finished.append(time.monotonic())
            yield self.body

    class FastResponse(FakeResponse):
        def iter_content(self, **kwargs):
            sent_fast.append(time.monotonic())
            yield self.body

    urls = [f"https://publisher.test/story/{i}" for i in range(6)]
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://slow.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://slow.test/story/1": DelayedResponse(article_html()),
              **{url: FastResponse(article_html()) for url in urls}}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": u} for u in urls] + [{"url": "https://slow.test/story/1"}])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0, continuous_budgets=True)
        assert result["states"] == {"complete": 7}
        assert len(sent_fast) == 6 and max(sent_fast) < finished[0]
        assert result["cycle"]["budget_rollovers"] >= 6
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        queue.close()
        http.close()


def test_image_heading_can_use_own_page_open_graph_title_without_fabricating_credit(config, tmp_path):
    config["providers"][0].update(require_configured_container=True, require_author_attribution=True,
                                  article_selectors=[".story"], title_selector="h1", author_selectors=[".byline a"])
    source = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    payload = ('<meta property="og:title" content="Actual page headline"><h1><img alt=""></h1>'
               '<div class="byline"><a>Known author</a><a>Known author</a></div><div class="story">' + 'Actual original text. ' * 15 + '</div>').encode()
    result = extract_article(payload, source, page_url="https://publisher.test/story/1")
    assert result["title"] == "Actual page headline" and json.loads(result["authors_json"]) == ["Known author"]


def test_removed_bulk_endpoint_is_not_reenabled_by_fulltext_feed(config, tmp_path):
    config["providers"][0]["fulltext_feeds"] = ["https://publisher.test/feed/"]
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/feed/": FakeResponse(rss([feed_item()]), headers={"Content-Type": "application/rss+xml"})}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.db.executemany("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'wordpress_posts','publisher',-1)",
                                 ((f"https://publisher.test/wp-json/wp/v2/posts?page={i}",) for i in range(1, 25)))
        removed = dict(queue.db.execute("SELECT * FROM discovery LIMIT 1").fetchone())
        with pytest.raises(NewsError, match="unregistered_bulk_api"):
            fetch_discovery(http, removed)
        assert not session.calls
        result = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1)
        assert result["articles_with_stored_body"] == 1
        assert all('wp-json' not in url for url, _ in session.calls)
        assert queue.db.execute("SELECT count(*) FROM discovery WHERE kind='wordpress_posts' AND attempts=0").fetchone()[0] == 24
    finally:
        queue.close()
        http.close()


def test_historical_fulltext_feed_yields_bounded_article_slot_without_losing_history(config, tmp_path):
    config.update(workers=1, bulk_article_burst=3)
    config['providers'][0].update(fulltext_feeds=['https://publisher.test/feed/'])
    article = 'https://publisher.test/html-only'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              article: FakeResponse(article_html())}
    for page in range(1, 9):
        routes[feed_url('https://publisher.test/feed/', page)] = FakeResponse(
            rss([feed_item(page)]) if page < 8 else rss([]), headers={'Content-Type': 'application/rss+xml'})
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': article}])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=8, continuous_budgets=True)
        calls = [url for url, _ in session.calls if not url.endswith('/robots.txt')]
        assert calls[:5] == [feed_url('https://publisher.test/feed/', p) for p in range(1, 5)] + [article]
        assert result['articles_with_stored_body'] == 8
        assert queue.metrics.snapshot()['providers']['publisher']['counts']['frontier_bulk_fairness_article_dispatches'] == 1
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


def test_current_release_feed_has_priority_even_when_history_burst_is_full(config, tmp_path):
    config.update(workers=1, bulk_article_burst=1)
    config['providers'][0].update(fulltext_feeds=['https://publisher.test/feed/'])
    article, base = 'https://publisher.test/html-only', 'https://publisher.test/feed/'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              base: FakeResponse(rss([feed_item()]), headers={'Content-Type': 'application/rss+xml'}),
              feed_url(base, 2): FakeResponse(rss([]), headers={'Content-Type': 'application/rss+xml'}),
              article: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    http.bulk_since_article['publisher'] = 1
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': article}])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=1, continuous_budgets=True)
        assert [url for url, _ in session.calls][:3] == ['https://publisher.test/robots.txt', base, article]
        assert result['articles_with_stored_body'] == 2
    finally:
        queue.close()
        http.close()


def test_malformed_xml_diagnosis_is_bounded_and_continues_other_endpoints(config, tmp_path, capsys):
    from scripts.probe_news_sources import probe_provider
    bad, good = 'https://publisher.test/bad-feed', 'https://publisher.test/good-feed'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              bad: FakeResponse(b'<rss><item>PRIVATE_REJECTED_TEXT &bad;</item></rss>', headers={'Content-Type': 'application/rss+xml'}),
              good: FakeResponse(rss([]), headers={'Content-Type': 'application/rss+xml'})}
    http, _ = http_fixture(config, tmp_path, routes)
    try:
        result = probe_provider(http, [bad, good])
        assert result[0]['error'] == 'invalid_feed' and result[0]['xml_parse_errors']
        assert set(result[0]['xml_parse_errors'][0]) == {'type', 'domain', 'line', 'column'}
        assert result[0]['cause_chain'] == []
        assert result[1]['status'] == 200 and result[1]['xml_items'] == 0
        assert 'PRIVATE_REJECTED_TEXT' not in json.dumps(result) + capsys.readouterr().out
    finally:
        http.close()


def test_fed_legacy_body_excludes_navigation_and_preserves_date_only(config, tmp_path):
    config["providers"][0].update(id="fed_board", domains=["federalreserve.gov"],
        authorization={"kind": "public_domain", "license": "US-government-public-domain", "scope": "Board text",
                       "evidence_url": "https://www.federalreserve.gov/disclaimer.htm"},
        legacy_release_layout="fed_table_release_v1", require_configured_container=True, article_selectors=["#article"])
    source = PolicyRegistry(config, base=tmp_path).sources["fed_board"]
    payload = ('<title>Actual Board release title</title><body><font><i>Release Date: December 27, 1996</i></font>'
               '<table width="560">For immediate release</table><table width="600"><tr><td><p>' +
               'The Board announced actual release information. ' * 20 + '</p><a href="#pagetop">Return to top</a></td></tr></table>'
               '<p>Other announcements</p><hr><font>Last update: December 31, 1996 12:00 PM</font></body>').encode()
    result = extract_article(payload, source, page_url="https://www.federalreserve.gov/boarddocs/press/general/1996/19961227/")
    assert result["published_at_raw"] == "December 27, 1996"
    assert 'date_only' in result["published_time_basis"]
    assert all(text not in result["body"] for text in ('Last update', 'Other announcements', 'Return to top'))
    with pytest.raises(NewsError, match="body_not_found"):
        extract_article(payload, source, page_url="https://www.federalreserve.gov/newsevents/press/all/1996all.htm")


def test_changed_index_contract_reparses_success_but_preserves_refusal_deadline(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        deadline = time.time() + 900
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,parser_fingerprint) VALUES (?,'index','publisher',?,'old')",
                             ('https://publisher.test/archive.htm', deadline))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,parser_fingerprint,error_code) "
                             "VALUES (?,'index','publisher',?,'old','source_access_denied')",
                             ('https://publisher.test/refused.htm', deadline))
        seed_discovery(queue)
        assert queue.db.execute("SELECT due FROM discovery WHERE url LIKE '%archive.htm'").fetchone()[0] == 0
        assert queue.db.execute("SELECT due FROM discovery WHERE url LIKE '%refused.htm'").fetchone()[0] == deadline
    finally:
        queue.close()
        http.close()


def test_verified_fed_multitable_annexes_are_complete_not_nested_duplicates(config, tmp_path):
    config["providers"][0].update(id="fed_board", domains=["federalreserve.gov"],
        authorization={"kind": "public_domain", "license": "US-government-public-domain", "scope": "Board text",
                       "evidence_url": "https://www.federalreserve.gov/disclaimer.htm"},
        legacy_release_layout="fed_table_release_v2", require_configured_container=True, article_selectors=["#article"])
    source = PolicyRegistry(config, base=tmp_path).sources["fed_board"]
    payload = ('<title>Declared Board release</title><body><i>Release Date: December 24, 1996</i>'
               '<table width="560"><tr><td>HEADER</td></tr></table><table width="555"><tr><td><p>' +
               'The Board released this actual declaration. ' * 10 + '</p></td></tr></table><hr>'
               '<table width="600"><tr><td><p>Regulatory annex unique words.</p><table><tr><td>NESTED ONCE</td></tr></table>'
               '</td></tr></table><p>Archive link excluded</p><font>Last update: December 26</font></body>').encode()
    result = extract_article(payload, source, page_url="https://www.federalreserve.gov/boarddocs/press/general/1996/19961224/")
    assert result["extraction_method"] == "fed_table_release_v2"
    assert result["body"].count("NESTED ONCE") == 1
    assert "Regulatory annex" in result["body"] and "HEADER" not in result["body"]
    assert "Archive link excluded" not in result["body"] and "Last update" not in result["body"]


def test_board_pdf_retains_original_text_not_creation_or_meeting_release_date(config, tmp_path):
    import pymupdf
    from downloader.news_fulltext_pdf import extract_board_pdf
    config["providers"][0].update(id="fed_board", domains=["federalreserve.gov"],
        authorization={"kind": "public_domain", "license": "US-government-public-domain", "scope": "Board text",
                       "evidence_url": "https://www.federalreserve.gov/disclaimer.htm"},
        pdf_path_pattern=r"^/monetarypolicy/files/FOMC[A-Za-z0-9_.-]+[.]pdf$")
    source = PolicyRegistry(config, base=tmp_path).sources["fed_board"]
    url = "https://www.federalreserve.gov/monetarypolicy/files/FOMChistmin19360318.pdf"
    with pymupdf.open() as document:
        page = document.new_page()
        page.insert_text((50, 50), "The Board discusses actual economic policy and market developments. " * 4)
        document.set_metadata({"title": "Declared official meeting", "creationDate": "D:20201010101010"})
        body = document.tobytes()
    result = extract_board_pdf(body, source, page_url=url)
    assert result["extraction_method"] == "fed_pdf_original_text_v1" and result["pdf_pages"] == 1
    assert result["published_at_raw"] is None and result["modified_at_raw"] is None
    assert "20201010" in result["pdf_metadata_raw_json"]
    assert "not_release_clock" in result["published_time_basis"]
    with pytest.raises(NewsError, match="unregistered_pdf_document"):
        extract_board_pdf(body, source, page_url="https://www.federalreserve.gov/other/unreviewed.pdf")
    with pymupdf.open() as document:
        document.new_page()
        blank = document.tobytes()
    with pytest.raises(NewsError, match="pdf_image_only_or_body_not_found"):
        extract_board_pdf(blank, source, page_url=url)


def test_registered_pdf_negotiation_and_size_budget_do_not_apply_to_other_pages(config, tmp_path):
    from test_news_fulltext import FakeResponse
    config["providers"][0].update(id="fed_board", domains=["federalreserve.gov"],
        authorization={"kind": "public_domain", "license": "US-government-public-domain", "scope": "Board text",
                       "evidence_url": "https://www.federalreserve.gov/disclaimer.htm"},
        pdf_path_pattern=r"^/monetarypolicy/files/FOMC[A-Za-z0-9_.-]+[.]pdf$", pdf_max_response_bytes=64)
    config["max_response_bytes"] = 8
    url = "https://www.federalreserve.gov/monetarypolicy/files/FOMChistmin19360318.pdf"
    other = "https://www.federalreserve.gov/unreviewed.pdf"
    http, session = http_fixture(config, tmp_path, {url: FakeResponse(b"%PDF-" + b"a" * 20), other: FakeResponse(b"a" * 20)})
    try:
        assert http._raw(url).body.startswith(b"%PDF-")
        assert session.calls[-1][1]["headers"]["Accept"] == "application/pdf"
        with pytest.raises(NewsError, match="response_too_large"):
            http._raw(other)
        assert session.calls[-1][1]["headers"]["Accept"] != "application/pdf"
    finally:
        http.close()


def test_legacy_v3_preserves_list_and_numeric_annexes_before_footer(config, tmp_path):
    config["providers"][0].update(id="fed_board", domains=["federalreserve.gov"],
        authorization={"kind": "public_domain", "license": "US-government-public-domain", "scope": "Board text",
                       "evidence_url": "https://www.federalreserve.gov/disclaimer.htm"},
        legacy_release_layout="fed_table_release_v3", require_configured_container=True, article_selectors=["#article"])
    source = PolicyRegistry(config, base=tmp_path).sources["fed_board"]
    payload = ('<title>Actual release</title><body><table width="600"><tr><td>OLD HEADER EXCLUDED</td></tr></table>'
        '<font><i>Release Date: March 27, 1997</i></font><table width="560"><tr><td>IMMEDIATE HEADER</td></tr></table>'
        '<table width="600"><tr><td><p>' + 'Original Board declaration. ' * 20 + '</p></td></tr></table>'
        '<table width="600"><tr><td><ul><li>Actual list-only annex</li></ul></td></tr></table>'
        '<table width="650"><tr><td>Numeric annex 4390 36 77</td></tr></table>'
        '<p><a>1997 Other announcements</a></p><table width="600"><tr><td>FOOTER EXCLUDED</td></tr></table>'
        '<font size="-1">Last update: March 28, 1997</font></body>').encode()
    result = extract_article(payload, source, page_url="https://www.federalreserve.gov/boarddocs/press/general/1997/19970327/")
    assert result["extraction_method"] == "fed_table_release_v3"
    assert "Actual list-only annex" in result["body"] and "4390" in result["body"]
    assert all(word not in result["body"] for word in ("FOOTER", "HEADER", "Other announcements", "Last update"))
    assert result["published_at_raw"] == "March 27, 1997"
    unknown_title = extract_article(payload.replace(b'<title>Actual release</title>', b''), source,
        page_url="https://www.federalreserve.gov/boarddocs/press/general/1997/19970327/")
    assert unknown_title["body"] == result["body"] and unknown_title["title"] == ""
    assert unknown_title["title_basis"] == "unknown_source_title_not_found"


def test_rss_incomplete_item_can_use_canonical_html_fallback(config, tmp_path):
    config["providers"][0]["fulltext_feeds"] = ["https://publisher.test/feed/"]
    config["providers"][0]["require_author_attribution"] = True
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/feed/": FakeResponse(rss([feed_item(1), feed_item(2, author="")]),
                                                           headers={"Content-Type": "application/rss+xml"})}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        response, links, records, errors = fetch_discovery(http, task)
        assert len(links) == 2 and len(records) == 1 and len(errors) == 1
        assert accept_discovery(queue, task, response, links, records, errors) == 1
        assert queue.db.execute("SELECT state FROM articles WHERE url='https://publisher.test/story/2'").fetchone()[0] == 'pending'
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        queue.close()
        http.close()


def test_all_rss_missing_credits_keeps_html_links_before_adapter_circuit(config, tmp_path):
    config["providers"][0]["fulltext_feeds"] = ["https://publisher.test/feed/"]
    config["providers"][0]["require_author_attribution"] = True
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/feed/": FakeResponse(rss([feed_item(1, author="")]), headers={"Content-Type": "application/rss+xml"})}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        seed_discovery(queue)
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        with pytest.raises(NewsError, match="bulk_all_items_unusable"):
            accept_discovery(queue, task, *fetch_discovery(http, task))
        assert queue.db.execute("SELECT url,state,body_sha256 FROM articles").fetchone()[:] == ('https://publisher.test/story/1', 'pending', None)
        assert not list(tmp_path.glob('bodies/**/*.parquet'))
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('owner_started', [997, 1002])
def test_runtime_audit_rejects_previous_owner_after_service_fork(monkeypatch, owner_started):
    from scripts.audit_news_runtime import owner_process_clock_check
    # time=2000, boot age=1100, PID start=100 seconds -> actual PID epoch=1000.
    monkeypatch.setattr('scripts.audit_news_runtime.time.time', lambda: 2000)
    monkeypatch.setattr('scripts.audit_news_runtime.time.clock_gettime', lambda _: 1100)
    monkeypatch.setattr('scripts.audit_news_runtime.os.sysconf', lambda _: 100)
    monkeypatch.setattr(Path, 'read_text', lambda *a, **k: '321 (python worker) ' + ' '.join(['S'] + ['0'] * 18 + ['10000']))
    status = {'updated_at_utc': datetime.fromtimestamp(2000, timezone.utc).isoformat(),
              'performance': {'owner_elapsed_seconds': 2000 - owner_started}}
    result = owner_process_clock_check({'MainPID': '321'}, status)
    assert result['accepted'] is (owner_started >= 998)


@pytest.mark.parametrize('owner_started,accepted', [(997, False), (1002, True)])
def test_runtime_audit_uses_locked_counter_clock_not_delayed_publication(monkeypatch, owner_started, accepted):
    from scripts.audit_news_runtime import owner_process_clock_check
    monkeypatch.setattr('scripts.audit_news_runtime.time.time', lambda: 2000)
    monkeypatch.setattr('scripts.audit_news_runtime.time.clock_gettime', lambda _: 1100)
    monkeypatch.setattr('scripts.audit_news_runtime.os.sysconf', lambda _: 100)
    monkeypatch.setattr(Path, 'read_text', lambda *a, **k: '321 (python worker) ' + ' '.join(['S'] + ['0'] * 18 + ['10000']))
    status = {'updated_at_utc': datetime.fromtimestamp(2005, timezone.utc).isoformat(),
              'performance': {'snapshot_clock_contract_version': 1,
                  'snapshot_at_utc': datetime.fromtimestamp(2000, timezone.utc).isoformat(),
                  'owner_elapsed_seconds': 2000 - owner_started}}
    result = owner_process_clock_check({'MainPID': '321'}, status)
    assert result['accepted'] is accepted
    assert result['owner_metric_started_at_epoch'] == owner_started
