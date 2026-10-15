import time
import sqlite3
from pathlib import Path

import pytest

from downloader.news_fulltext import run_cycle, seed_discovery
from downloader.news_fulltext_cohorts import cohort_progress, membership_digest
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.plan_news_sources import plan_sources
from scripts.audit_news_finite_cohorts import audit_cohorts
from test_news_fulltext import FakeResponse, article_html, config, http_fixture
from downloader.news_fulltext import load_config


def finite(config):
    config["providers"][0]["acquisition_mode"] = "finite"
    return config


def test_empty_then_freeze_exact_set_and_later_growth(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(finite(config), base=tmp_path))
    try:
        assert q.db.execute("SELECT count(*) FROM finite_cohorts").fetchone()[0] == 0
        with q.db:
            q.add_articles([{"url": "https://publisher.test/a"}, {"url": "https://publisher.test/b"}])
        q.sync_finite_cohorts()
        original = dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone())
        with q.db:
            q.add_articles([{"url": "https://publisher.test/c"}, {"url": "https://publisher.test/a"}])
        q.sync_finite_cohorts()
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
        assert q.db.execute("SELECT state FROM articles WHERE url LIKE '%/c'").fetchone()[0] == "waiting_cohort"
        assert q.claim_url("https://publisher.test/c", 300, refresh=True) is None
        p = cohort_progress(q.db, "publisher")
        assert p["urls"] == 2 and p["new_urls_outside_cohort"] == 1
        assert not p["all_cohort_bodies_stored"] and p["body_progress_pct"] == 0
        assert p["urls_sha256"] == membership_digest(q.db, "publisher")[1]
    finally:
        q.close()


def test_explicit_batch_defers_only_new_cohort_until_all_selected_urls_arrive(config, tmp_path):
    registry = PolicyRegistry(finite(config), base=tmp_path)
    old = NewsQueue(tmp_path, registry)
    with old.db:
        old.add_articles([{'url': 'https://publisher.test/a'}])
    old.close()  # Retained/routed metadata, but no frozen initial cohort yet.
    selected = NewsQueue(tmp_path, registry, defer_initial_finite_freeze=True)
    try:
        assert selected.db.execute('SELECT count(*) FROM finite_cohorts').fetchone()[0] == 0
        with selected.db:
            selected.add_articles([{'url': 'https://publisher.test/b'}, {'url': 'https://publisher.test/c'}])
        selected.sync_finite_cohorts()
        assert selected.db.execute('SELECT urls FROM finite_cohorts').fetchone()[0] == 3
        assert selected.db.execute("SELECT count(*) FROM articles WHERE state='waiting_cohort'").fetchone()[0] == 0
        assert membership_digest(selected.db, 'publisher')[0] == 3
    finally:
        selected.close()


def test_explicit_batch_deferral_never_rewrites_an_existing_frozen_receipt(config, tmp_path):
    registry = PolicyRegistry(finite(config), base=tmp_path)
    old = NewsQueue(tmp_path, registry)
    with old.db:
        old.add_articles([{'url': 'https://publisher.test/a'}])
    old.sync_finite_cohorts()
    original = dict(old.db.execute('SELECT * FROM finite_cohorts').fetchone())
    original_bytes = (tmp_path / original['receipt_path']).read_bytes()
    old.close()
    selected = NewsQueue(tmp_path, registry, defer_initial_finite_freeze=True)
    try:
        with selected.db:
            selected.add_articles([{'url': 'https://publisher.test/b'}])
        selected.sync_finite_cohorts()
        assert dict(selected.db.execute('SELECT * FROM finite_cohorts').fetchone()) == original
        assert (tmp_path / original['receipt_path']).read_bytes() == original_bytes
        assert selected.db.execute("SELECT state FROM articles WHERE url='https://publisher.test/b'").fetchone()[0] == 'waiting_cohort'
        assert selected.claim_url('https://publisher.test/b', 300) is None
    finally:
        selected.close()


@pytest.mark.parametrize('invalid', [None, 1, 0, 'true'])
def test_initial_freeze_deferral_is_an_explicit_boolean(config, tmp_path, invalid):
    with pytest.raises(ValueError, match='freeze_switch_must_be_boolean'):
        NewsQueue(tmp_path, PolicyRegistry(finite(config), base=tmp_path), defer_initial_finite_freeze=invalid)


def test_restart_preserves_cohort_and_refusal_deadline(config, tmp_path):
    registry = PolicyRegistry(finite(config), base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    q.sync_finite_cohorts()
    old = dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone())
    deadline = time.time() + 300
    with q.db:
        q.db.execute("UPDATE articles SET state='retry',due=?,error_code='http_429'", (deadline,))
    q.close()
    resumed = NewsQueue(tmp_path, registry)
    try:
        assert dict(resumed.db.execute("SELECT * FROM finite_cohorts").fetchone()) == old
        assert resumed.claim("publisher", 300) is None
        assert resumed.db.execute("SELECT due,error_code FROM articles").fetchone()[:] == (deadline, "http_429")
        assert resumed.status()["providers"]["publisher"]["finite_cohort"]["urls"] == 1
    finally:
        resumed.close()


def test_finite_no_discovery_recheck_or_repeat_capture(config, tmp_path):
    finite(config)["providers"][0]["feeds"] = ["https://publisher.test/rss"]
    config["providers"][0]["index_pages"] = ["https://publisher.test/index"]
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(article_html()),
    })
    q = NewsQueue(tmp_path, http.registry)
    try:
        with q.db:
            q.add_articles([{"url": "https://publisher.test/a", "date": "2026-10-09"}])
            q.db.execute("INSERT INTO discovery(url,kind,provider) VALUES ('https://publisher.test/rss','feed','publisher')")
        result = run_cycle(q, http, config, max_articles=3, max_discovery_requests=3)
        assert result["states"] == {"complete": 1}
        assert result["providers"]["publisher"]["finite_cohort"]["all_cohort_bodies_stored"]
        with q.db:
            q.db.execute("UPDATE articles SET last_checked_at_utc='2000-01-01T00:00:00+00:00'")
        q.schedule_recent_rechecks(1, 10000)
        seed_discovery(q)
        run_cycle(q, http, config, max_articles=3, max_discovery_requests=3)
        assert len(session.calls) == 2  # One robots plus one body, never the feed/index.
        assert verify_bodies(tmp_path)["accepted"]
        proof = audit_cohorts(tmp_path)
        assert proof["scope_integrity_accepted"] and proof["all_cohort_bodies_stored"]
        assert proof["cohorts"][0]["batch_proof"]["current_heads_verified"] == 1
        assert proof["cohorts"][0]["stored_heads_with_timezone_aware_iso_clock"] == 0
        assert proof["cohorts"][0]["first_declared_publication_utc"] is None
        assert not proof["cohorts"][0]["publication_bounds_cover_all_stored_heads"]
    finally:
        http.close()
        q.close()


@pytest.mark.parametrize(("clock", "expected_utc"), [
    ("2014-02-03T09:00:00+08:00", "2014-02-03T01:00:00+00:00"),
    ("2014-02-03T01:00:00Z", "2014-02-03T01:00:00+00:00"),
    ("2014-02-03T09:00:00", None),
    ("2014/02/03 09:00", None),
])
def test_cohort_publication_bounds_require_declared_timezone(config, tmp_path, clock, expected_utc):
    finite(config)
    html = article_html().replace(b"2014-02-03", clock.encode())
    http, _ = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nDisallow:\n"),
        "https://publisher.test/a": FakeResponse(html),
    })
    q = NewsQueue(tmp_path, http.registry)
    try:
        with q.db:
            q.add_articles([{"url": "https://publisher.test/a"}])
        run_cycle(q, http, config, max_articles=1, max_discovery_requests=0)
        proof = audit_cohorts(tmp_path)["cohorts"][0]
        assert proof["accepted"]
        assert proof["stored_heads_with_declared_clock"] == 1
        assert proof["publication_clock_examples_raw"] == [clock]
        assert proof["stored_heads_with_timezone_aware_iso_clock"] == int(expected_utc is not None)
        assert proof["first_declared_publication_utc"] == expected_utc
        assert proof["last_declared_publication_utc"] == expected_utc
        assert proof["publication_bounds_cover_all_stored_heads"] is (expected_utc is not None)
    finally:
        http.close()
        q.close()


def test_missing_and_terminal_failures_are_not_scope_completion(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(finite(config), base=tmp_path))
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}, {"url": "https://publisher.test/b"}])
    q.sync_finite_cohorts()
    with q.db:
        q.db.execute("UPDATE articles SET state='robots_disallowed' WHERE url LIKE '%/b'")
    q.close()
    proof = audit_cohorts(tmp_path)
    assert proof["scope_integrity_accepted"]
    assert not proof["all_cohort_bodies_stored"] and not proof["cohorts"][0]["accepted"]
    assert proof["cohorts"][0]["remaining_missing_bodies"] == 2


def test_tampered_membership_rejected_and_failed_start_releases_owner_lock(config, tmp_path):
    registry = PolicyRegistry(finite(config), base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    q.sync_finite_cohorts()
    q.close()
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as db:
        db.execute("UPDATE finite_cohort_members SET url='https://publisher.test/tampered'")
    assert not audit_cohorts(tmp_path)["scope_integrity_accepted"]
    for _ in range(2):
        with pytest.raises(ValueError, match="receipt/membership mismatch"):
            NewsQueue(tmp_path, registry)  # Must not become owner_already_running.


def test_owner_restart_requeues_inflight_only_inside_exact_cohort(config, tmp_path):
    registry = PolicyRegistry(finite(config), base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    q.sync_finite_cohorts()
    row = q.claim('publisher', 300)
    assert row is not None
    q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert q.claim('publisher', 300)['url'] == row['url']
        assert cohort_progress(q.db, 'publisher')['urls'] == 1
    finally:
        q.close()


def test_continuous_transition_releases_only_growth(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(finite(config), base=tmp_path))
    with q.db:
        q.add_articles([{"url": "https://publisher.test/a"}])
    q.sync_finite_cohorts()
    with q.db:
        q.add_articles([{"url": "https://publisher.test/b"}])
        q.db.execute("UPDATE articles SET state='robots_disallowed',error_code='robots_disallowed' WHERE url LIKE '%/a'")
    q.close()
    config["providers"][0]["acquisition_mode"] = "continuous"
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert q.db.execute("SELECT state FROM articles WHERE url LIKE '%/b'").fetchone()[0] == "pending"
        assert q.db.execute("SELECT state FROM articles WHERE url LIKE '%/a'").fetchone()[0] == "robots_disallowed"
        assert q.db.execute("SELECT count(*) FROM finite_cohorts").fetchone()[0] == 1
    finally:
        q.close()


def test_finite_does_not_override_disabled_policy(config, tmp_path):
    finite(config)["providers"][0]["enabled"] = False
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([{"url": "https://publisher.test/a"}])
        q.sync_finite_cohorts()
        assert q.db.execute("SELECT count(*) FROM finite_cohorts").fetchone()[0] == 0
        assert q.db.execute("SELECT state FROM articles").fetchone()[0] == "awaiting_authorization"
    finally:
        q.close()


@pytest.mark.parametrize("mode", ["all", "", True, "FINITE", [], {}])
def test_unknown_acquisition_mode_rejected(config, tmp_path, mode):
    config["providers"][0]["acquisition_mode"] = mode
    with pytest.raises(ValueError, match="acquisition mode"):
        PolicyRegistry(config, base=tmp_path)


def test_declared_publication_selector_is_scoped_unique_and_never_inferred(config, tmp_path):
    entry = config['providers'][0]
    entry['publication_selectors'] = ['article > header .released']
    source = PolicyRegistry(config, base=tmp_path).sources['publisher']
    payload = ('<h1>Title</h1><article><header><span class="released">2026/10/09 09:00</span></header>'
               '<p>' + '可公開的原始資料必須依真正的發布時間供研究使用。' * 15 + '</p></article>'
               '<aside><time datetime="2000-01-01">Other story</time></aside>').encode()
    assert extract_article(payload, source)['published_at_raw'] == '2026/10/09 09:00'
    ambiguous = payload.replace(b'</header>', b'<span class="released">2026/10/10 09:00</span></header>')
    assert extract_article(ambiguous, source)['published_at_raw'] is None


def test_abmedia_article_container_excludes_advertisements_and_navigation():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['abmedia']
    payload = ('<article class="page-single"><header><h1>Fictional article fixture</h1>'
        '<a href="/author/fixture/">Fixture Author</a></header><div class="desc">'
        '<p>' + '測試用原文段落並非真實新聞內容。' * 20 + '</p>'
        '<div class="abmed-entity-placement">AD_FIXTURE</div><div id="ez-toc-container">TOC_FIXTURE</div>'
        '<div class="post-note">RISK_FOOTER_FIXTURE</div><div class="exts">RELATED_FIXTURE</div>'
        '</div></article><aside>UNRELATED_FIXTURE</aside>').encode()
    article = extract_article(payload, source, page_url='https://abmedia.io/fixture')
    assert article['authors_json'] == '["Fixture Author"]'
    assert not any(marker in article['body'] for marker in ('AD_FIXTURE', 'TOC_FIXTURE', 'RELATED_FIXTURE',
                                                          'RISK_FOOTER_FIXTURE', 'UNRELATED_FIXTURE'))
    assert article['published_at_raw'] is None


def test_peopo_own_article_license_clock_and_byline_not_related_story():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['peopo']
    payload = ('<div id="block-peopo-content"><article class="node--view-mode-full">'
        '<header><h1>Fictional CC article fixture</h1><div class="node__meta">'
        '<span class="me-1">2026/10/09 09:00</span></div></header><div class="node-pre-content">'
        '<div class="view-node-field"><span class="license-logo">'
        '<a href="http://creativecommons.org/licenses/by/3.0/tw/">ARTICLE_LICENSE</a></span></div></div>'
        '<div class="node__content"><div class="field--name-body">' + '測試用開放授權文章而非真實新聞。' * 20 + '</div></div>'
        '</article><article><time datetime="2010-01-01">RELATED_CLOCK</time><div class="field--name-body">RELATED_BODY</div></article></div>'
        '<div id="sidebar_first"><div id="block-views-block-node-field-block-3"><div class="user-cards">'
        '<h3><a class="username">Fixture Author</a></h3></div></div><a class="username">OTHER_WRITER</a></div>'
        '<footer><a href="http://creativecommons.org/licenses/by/3.0/tw/">FOOTER_LICENSE</a></footer>').encode()
    article = extract_article(payload, source, page_url='https://www.peopo.org/news/1')
    assert article['published_at_raw'] == '2026/10/09 09:00'
    assert article['authors_json'] == '["Fixture Author"]' and article['per_article_license_verified']
    assert 'RELATED_BODY' not in article['body']
    # A footer license is insufficient when this article retains its rights.
    refused = payload.replace(b'<a href="http://creativecommons.org/licenses/by/3.0/tw/">ARTICLE_LICENSE</a>',
                              '版權所有'.encode())
    with pytest.raises(NewsError, match='article_license_not_verified'):
        extract_article(refused, source, page_url='https://www.peopo.org/news/1')


def test_planner_retains_every_role_not_top_k_or_fake_completion():
    items = {
        "gov": {"source_role": "government_origin", "known_urls": 4},
        "fed_board": {"source_role": "government_origin", "known_urls": 20},
        "cnews": {"source_role": "publisher_or_other_unverified", "known_urls": 70},
        "large": {"source_role": "publisher_or_other_unverified", "known_urls": 9000},
        "redirect": {"source_role": "redirector", "known_urls": 10},
        "social": {"source_role": "social_or_video", "known_urls": 1},
        "current": {"source_role": "publisher_or_other_unverified", "known_urls": 300, "enabled": True},
    }
    result = plan_sources({"providers": items, "observed_at_utc": "2026-10-09T00:00:00Z"})
    rows = {r["provider"]: r for r in result["sources"]}
    assert len(rows) == len(items)
    assert rows["cnews"]["strategy"] == "review_then_finite_existing_metadata"
    assert rows["redirect"]["stage"] == 4 and rows["social"]["stage"] == 5
    assert rows["fed_board"]["priority_tier"] < rows["cnews"]["priority_tier"] < rows["gov"]["priority_tier"]
    assert rows["gov"]["research_topic"] == "public_sector_unclassified"
    assert result["contract_version"] == 5
    assert all(r["finish_eta_utc"] is None and not r["dedicated_refresh_service"] for r in rows.values())
    assert not result["plan_is_acquisition_acceptance"] and not result["all_history_complete"]


def test_shared_bounded_workers_can_have_independent_larger_provider_ceilings(config, tmp_path):
    from downloader.news_fulltext_capacity import capacity_config
    config["adaptive_rate"] = {"enabled": True}
    config["workers"] = 1
    config["providers"][0]["capacity_max_parallelism"] = 3
    config["require_provider_worker_reservations"] = False
    assert capacity_config(config, PolicyRegistry(config, base=tmp_path))["enabled"]
    config["require_provider_worker_reservations"] = True
    with pytest.raises(ValueError, match="cannot reserve"):
        capacity_config(config, PolicyRegistry(config, base=tmp_path))
