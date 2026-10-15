"""CNEWS local URL growth is not discovery, a parser repair or a body grant."""
from copy import deepcopy
from dataclasses import replace
import os
from pathlib import Path
import time

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import load_config, run_cycle, seed_discovery
from downloader.news_fulltext_cohorts import membership_digest
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.audit_news_selected_articles import audit
from test_news_cnews_cmmedia import CNEWS_URL, cnews_page, schema
from test_news_fulltext import FakeResponse, http_fixture


NEW_URL = 'https://cnews.com.tw/204260806a06/'
DISCOVERY = ('feeds', 'sitemaps', 'wordpress_apis', 'cnyes_apis', 'fulltext_feeds',
             'index_pages', 'incremental_index_pages', 'metadata_feeds', 'cnyes_public_index')


def scoped_config(*, finite=False):
    cfg, _, _ = load_config(Path(os.environ.get('STOCKAGENT_NEWS_SCOPE_CONFIG', 'configs/news_fulltext.json')))
    entry = deepcopy(next(p for p in cfg['providers'] if p['id'] == 'cnews'))
    entry['acquisition_mode'] = 'finite' if finite else 'continuous'
    if finite:
        entry.pop('private_known_url_scope', None)
    else:
        entry['private_known_url_scope'] = 'personal_noncommercial_originals_v1'
    return {**cfg, 'providers': [entry]}


def test_continuous_cnews_keeps_exact_reviewed_native_boundaries():
    source = PolicyRegistry(scoped_config(), base=Path.cwd()).sources['cnews']
    assert source.allowed and source.known_url_only and source.acquisition_mode == 'continuous'
    assert source.domains == ('cnews.com.tw',)
    assert source.authorization['kind'] == 'personal_research'
    assert source.authorization['evidence_url'] == 'https://cnews.com.tw/%E7%89%88%E6%AC%8A%E8%81%B2%E6%98%8E/'
    assert source.require_author_attribution and source.require_configured_container
    assert source.embedded_article_layout == 'cnews_visible_article_v1'
    assert source.selectors == ("article[id^='post-']",)
    assert not any((source.feeds, source.sitemaps, source.index_pages, source.wordpress_apis,
                    source.cnyes_apis, source.fulltext_feeds, source.metadata_feeds, source.incremental_index_pages))


@pytest.mark.parametrize('field', DISCOVERY)
def test_cnews_cannot_turn_known_url_scope_into_any_discovery_route(field):
    cfg = scoped_config()
    cfg['providers'][0][field] = ['https://cnews.com.tw/endpoint']
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('field,value', [
    ('id', 'other_publisher'), ('domains', ['other.test']),
    ('require_configured_container', False), ('require_author_attribution', False),
    ('article_selectors', []), ('article_path_pattern', ''),
    ('private_known_url_scope', ''), ('private_known_url_scope', None),
    ('private_known_url_scope', True), ('private_known_url_scope', 'unreviewed'),
    ('private_known_url_scope', 'per_article_licensed_text_v1'),
    ('acquisition_mode', 'unreviewed'),
    ('authorization', {'kind': 'unknown'}),
    ('authorization', {'kind': 'personal_research', 'evidence_url': 'https://other.test/terms'}),
])
def test_scope_opt_in_is_not_a_generic_publisher_or_authorization_grant(field, value):
    cfg = scoped_config()
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


def test_finite_annotation_alone_does_not_extend_frozen_scope(tmp_path):
    cfg = scoped_config(finite=True)
    cfg['providers'][0]['private_known_url_scope'] = 'personal_noncommercial_originals_v1'
    queue = NewsQueue(tmp_path, PolicyRegistry(cfg, base=Path.cwd()))
    try:
        assert not queue.registry.sources['cnews'].known_url_only
        with queue.db:
            queue.add_articles([{'url': CNEWS_URL}])
        queue.sync_finite_cohorts()
        with queue.db:
            queue.add_articles([{'url': NEW_URL}])
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (NEW_URL,)).fetchone()[0] == 'waiting_cohort'
    finally:
        queue.close()


@pytest.mark.parametrize('state,error', [
    ('retry', 'http_429'), ('retry_cooldown', 'http_403'), ('robots_disallowed', 'robots_disallowed'),
    ('paywall_or_login', 'paywall_or_login'), ('source_not_found', 'source_not_found'),
    ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded'),
    ('article_attribution_not_verified', 'article_attribution_not_verified'),
    ('article_identity_not_verified', 'article_identity_not_verified'), ('body_not_found', 'body_not_found'),
])
def test_only_waiting_scope_releases_preserving_deadline_attempt_error_lease_and_frozen_receipt(tmp_path, state, error):
    finite = NewsQueue(tmp_path, PolicyRegistry(scoped_config(finite=True), base=Path.cwd()))
    with finite.db:
        finite.add_articles([{'url': CNEWS_URL}])
    finite.sync_finite_cohorts()
    with finite.db:
        finite.add_articles([{'url': NEW_URL, 'title': '本機真實標題', 'date': '2014-01-02'}])
        finite.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=?,lease_owner=?,lease_until=?,'
                         'last_checked_at_utc=? WHERE url=?',
                         (state, error, time.time() + 7200, 'original_owner', time.time() + 7200,
                          '2026-10-09T01:02:03+00:00', CNEWS_URL))
        # A due time and old diagnostic witnesses must survive scope release.
        finite.db.execute('UPDATE articles SET due=?,attempts=2,error_code=?,last_checked_at_utc=? WHERE url=?',
                         (time.time() + 7200, 'old_scope_witness', '2026-10-09T03:04:05+00:00', NEW_URL))
    protected = dict(finite.db.execute('SELECT * FROM articles WHERE url=?', (CNEWS_URL,)).fetchone())
    waiting = dict(finite.db.execute('SELECT * FROM articles WHERE url=?', (NEW_URL,)).fetchone())
    header = dict(finite.db.execute("SELECT * FROM finite_cohorts WHERE provider='cnews'").fetchone())
    receipt_sha = sha256_file(tmp_path / header['receipt_path'])
    digest = membership_digest(finite.db, 'cnews')
    finite.close()
    for _ in range(2):
        queue = NewsQueue(tmp_path, PolicyRegistry(scoped_config(), base=Path.cwd()))
        try:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (CNEWS_URL,)).fetchone()) == protected
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (NEW_URL,)).fetchone()) == {**waiting, 'state': 'pending'}
            assert dict(queue.db.execute("SELECT * FROM finite_cohorts WHERE provider='cnews'").fetchone()) == header
            assert sha256_file(tmp_path / header['receipt_path']) == receipt_sha
            assert membership_digest(queue.db, 'cnews') == digest
            assert queue.claim_url(CNEWS_URL, 300) is None
            assert queue.claim_url(NEW_URL, 300) is None
        finally:
            queue.close()


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
def test_schedule_change_preserves_original_body_byline_clock_and_parser_repair_identity(backend):
    before = PolicyRegistry(scoped_config(finite=True), base=Path.cwd()).sources['cnews']
    after = PolicyRegistry(scoped_config(), base=Path.cwd()).sources['cnews']
    assert before.data_fingerprint != after.data_fingerprint
    assert before.parser_repair_fingerprint == after.parser_repair_fingerprint
    assert before.http_capacity_source_fingerprint == after.http_capacity_source_fingerprint
    old = extract_article(cnews_page(), replace(before, html_parser=backend), page_url=CNEWS_URL)
    new = extract_article(cnews_page(), replace(after, html_parser=backend), page_url=CNEWS_URL)
    for field in ('body', 'body_sha256', 'title', 'published_at_raw', 'authors_json',
                  'author_attribution_detail_json', 'extraction_method', 'body_completeness'):
        assert old[field] == new[field]


@pytest.mark.parametrize('changes,code', [
    ({'credit': '第三方媒體授權轉載記者謝東明／台北報導'}, 'article_rights_or_scope_excluded'),
    ({'profile': 'https://other.test/author/cnews204/'}, 'article_rights_or_scope_excluded'),
    ({'author': '另一個作者'}, 'article_attribution_not_verified'),
    ({'clock': '2026-08-07T18:53:44+08:00'}, 'invalid_bulk_publication_clock'),
    ({'canonical': NEW_URL}, 'article_identity_not_verified'),
    ({'extra': '<div class="paywall">付費</div>'}, 'paywall_or_login'),
    ({'schema': schema(CNEWS_URL, free=False)}, 'paywall_or_login'),
    ({'body_access': '<form action="/login"><input type="password"></form>'}, 'paywall_or_login'),
])
def test_incremental_scope_does_not_relax_public_original_guards(changes, code):
    source = PolicyRegistry(scoped_config(), base=Path.cwd()).sources['cnews']
    with pytest.raises(NewsError, match=code):
        extract_article(cnews_page(**changes), source, page_url=CNEWS_URL)


def test_preexisting_discovery_kept_as_evidence_but_never_dispatched(tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(scoped_config(), base=Path.cwd()))
    try:
        with queue.db:
            for kind in ('feed', 'sitemap', 'index', 'metadata_feed', 'wordpress_posts', 'rss_fulltext', 'cnyes_news'):
                queue.db.execute('INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,0)',
                                 ('https://cnews.com.tw/' + kind, kind, 'cnews'))
        original = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        seed_discovery(queue)
        assert queue.discovery_candidates(queue.registry.sources['cnews'], 10) == []
        sql, binds = queue.discovery_candidates_query(queue.registry.sources['cnews'], 10)
        assert 'INDEXED BY' not in sql and list(queue.db.execute(sql, binds)) == []
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == original
    finally:
        queue.close()


def test_full_chain_only_new_body_uses_new_capture_contract_no_old_url_refetch(tmp_path):
    old_cfg = scoped_config(finite=True)
    old_http, old_session = http_fixture(old_cfg, tmp_path, {
        'https://cnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nDisallow:\n'),
        CNEWS_URL: FakeResponse(cnews_page()),
    })
    old = NewsQueue(tmp_path, old_http.registry)
    try:
        with old.db:
            old.add_articles([{'url': CNEWS_URL}])
        old.sync_finite_cohorts()
        assert run_cycle(old, old_http, old_cfg, max_articles=1, max_discovery_requests=0)['providers']['cnews']['states']['complete'] == 1
        with old.db:
            old.add_articles([{'url': NEW_URL}])
        head = dict(old.db.execute('SELECT * FROM articles WHERE url=?', (CNEWS_URL,)).fetchone())
        header = dict(old.db.execute("SELECT * FROM finite_cohorts WHERE provider='cnews'").fetchone())
        receipt_sha = sha256_file(tmp_path / header['receipt_path'])
        old_record = next(r for r in pq.read_table(tmp_path / head['parquet_path']).to_pylist() if r['requested_url'] == CNEWS_URL)
    finally:
        old_http.close()
        old.close()
    cfg = scoped_config()
    http, session = http_fixture(cfg, tmp_path, {
        'https://cnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nDisallow:\n'),
        NEW_URL: FakeResponse(cnews_page(canonical=NEW_URL, og_url=NEW_URL)),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (CNEWS_URL,)).fetchone()) == head
        assert run_cycle(queue, http, cfg, max_articles=1, max_discovery_requests=0)['providers']['cnews']['states']['complete'] == 2
        assert CNEWS_URL not in [url for url, _ in session.calls]
        new_head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (NEW_URL,)).fetchone())
        new_record = next(r for r in pq.read_table(tmp_path / new_head['parquet_path']).to_pylist() if r['requested_url'] == NEW_URL)
        assert new_record['data_fingerprint'] == http.registry.sources['cnews'].data_fingerprint
        assert old_record['data_fingerprint'] != new_record['data_fingerprint']
        assert old_record['body_sha256'] == new_record['body_sha256']
        assert old_record['published_at_raw'] == new_record['published_at_raw']
        assert dict(queue.db.execute("SELECT * FROM finite_cohorts WHERE provider='cnews'").fetchone()) == header
        assert sha256_file(tmp_path / header['receipt_path']) == receipt_sha
        assert verify_bodies(tmp_path)['accepted']
        readback = audit(tmp_path, [CNEWS_URL, NEW_URL], registry=http.registry)
        assert readback['accepted'], readback['errors']
        assert [r['capture_contract_matches_current_source'] for r in readback['articles']] == [False, True]
        with queue.db:
            queue.add_articles([{'url': 'https://cnews.com.tw/204260803a10/'}])
        assert queue.db.execute("SELECT state FROM articles WHERE url='https://cnews.com.tw/204260803a10/'").fetchone()[0] == 'pending'
    finally:
        http.close()
        queue.close()


def test_deployment_scope_and_other_providers_are_not_changed_by_candidate():
    cfg, before, repo = load_config(Path('configs/news_fulltext.json'))
    cfg = deepcopy(cfg)
    cnews = next(p for p in cfg['providers'] if p['id'] == 'cnews')
    cnews.update(acquisition_mode='continuous', private_known_url_scope='personal_noncommercial_originals_v1')
    after = PolicyRegistry(cfg, base=repo)
    assert before.sources.keys() == after.sources.keys()
    assert after.sources['cnews'].known_url_only
    assert all(before.sources[p] == after.sources[p] for p in before.sources if p != 'cnews')
