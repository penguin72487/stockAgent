"""Reviewed local URLs progress without discovering a site or inventing permission."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import time

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import load_config, seed_discovery
from downloader.news_fulltext_cohorts import membership_digest
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_cnews_cmmedia import CMMEDIA_URL, cmmedia_page
from test_news_fulltext import config


PROVIDERS = ('cmmedia', 'peopo')
DISCOVERY_FIELDS = ('feeds', 'sitemaps', 'wordpress_apis', 'cnyes_apis',
                    'fulltext_feeds', 'index_pages', 'incremental_index_pages',
                    'metadata_feeds', 'cnyes_public_index')


def scoped_config(provider, *, mode='continuous'):
    cfg = load_config(Path('configs/news_fulltext.json'))[0]
    entry = deepcopy(next(p for p in cfg['providers'] if p['id'] == provider))
    entry['acquisition_mode'] = mode
    entry['require_configured_container'] = True
    if mode == 'continuous':
        entry['private_known_url_scope'] = ('personal_noncommercial_originals_v1'
                                          if provider == 'cmmedia' else 'per_article_licensed_text_v1')
    else:
        entry.pop('private_known_url_scope', None)
    return {**cfg, 'providers': [entry]}


@pytest.mark.parametrize('provider', PROVIDERS)
def test_reviewed_incremental_admission_keeps_native_body_and_per_article_rights(provider):
    cfg = scoped_config(provider)
    source = PolicyRegistry(cfg, base=Path.cwd()).sources[provider]
    assert source.allowed and source.known_url_only and source.acquisition_mode == 'continuous'
    assert source.require_author_attribution and source.require_configured_container
    assert source.article_path_pattern and source.selectors
    assert not any((source.feeds, source.sitemaps, source.index_pages, source.wordpress_apis,
                    source.cnyes_apis, source.fulltext_feeds, source.metadata_feeds))
    if provider == 'peopo':
        assert source.required_license_url_prefix == 'http://creativecommons.org/licenses/'
        assert source.license_selectors == (
            '#block-peopo-content article.node--view-mode-full > .node-pre-content .view-node-field .license-logo a[href]',)
    else:
        assert source.embedded_article_layout == 'cmmedia_visible_article_v1'
        assert source.selectors == ('.article_content[itemprop=articleBody]',)


@pytest.mark.parametrize('provider', PROVIDERS)
@pytest.mark.parametrize('field', DISCOVERY_FIELDS)
def test_known_only_scope_rejects_every_bulk_feed_or_index_even_when_dormant(provider, field):
    cfg = scoped_config(provider)
    cfg['providers'][0][field] = ['https://invalid.test/endpoint']
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('provider', PROVIDERS)
@pytest.mark.parametrize('field,value', [
    ('private_known_url_scope', 'unreviewed'), ('private_known_url_scope', True),
    ('private_known_url_scope', None), ('acquisition_mode', 'unknown'),
    ('private_known_url_scope', []), ('private_known_url_scope', {}),
    ('require_author_attribution', False), ('article_selectors', []),
    ('article_path_pattern', ''), ('authorization', {'kind': 'unknown'}),
])
def test_known_only_flag_is_not_a_generic_access_or_layout_grant(provider, field, value):
    cfg = scoped_config(provider)
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('field,value', [
    ('required_license_url_prefix', ''), ('license_selectors', []),
    ('require_configured_container', False),
])
def test_per_article_license_scope_cannot_remove_native_grain_checks(field, value):
    cfg = scoped_config('peopo')
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError, match='known-URL-only'):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('provider', PROVIDERS)
def test_preexisting_dormant_discovery_is_kept_but_never_dispatched(provider, tmp_path):
    cfg = scoped_config(provider)
    queue = NewsQueue(tmp_path, PolicyRegistry(cfg, base=Path.cwd()))
    try:
        with queue.db:
            for kind in ('feed', 'sitemap', 'index', 'metadata_feed', 'wordpress_posts', 'rss_fulltext', 'cnyes_news'):
                queue.db.execute('INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,0)',
                                 ('https://example.test/' + kind, kind, provider))
        before = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        seed_discovery(queue)
        assert queue.discovery_candidates(queue.registry.sources[provider], 10) == []
        statement, binds = queue.discovery_candidates_query(queue.registry.sources[provider], 10)
        assert 'INDEXED BY' not in statement  # WHERE0 cannot imply the partial index.
        assert list(queue.db.execute(statement, binds)) == []
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    finally:
        queue.close()


@pytest.mark.parametrize('provider', PROVIDERS)
@pytest.mark.parametrize('state,error', [
    ('robots_disallowed', 'robots_disallowed'), ('paywall_or_login', 'paywall_or_login'),
    ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded'),
    ('retry', 'http_429'), ('retry_cooldown', 'http_403'),
])
def test_only_scope_wait_releases_and_restart_preserves_receipts_deadlines_and_leases(provider, state, error, tmp_path):
    urls = [CMMEDIA_URL, 'https://www.cmmedia.com.tw/home/articles/48369'] if provider == 'cmmedia' else [
        'https://www.peopo.org/news/1', 'https://www.peopo.org/news/2']
    old = NewsQueue(tmp_path, PolicyRegistry(scoped_config(provider, mode='finite'), base=Path.cwd()))
    with old.db:
        old.add_articles([{'url': urls[0]}])
    old.sync_finite_cohorts()
    with old.db:
        old.add_articles([{'url': urls[1], 'title': '真實本機來源標題', 'date': '2014-01-02'}])
        old.db.execute('UPDATE articles SET state=?,error_code=?,due=?,attempts=3,lease_owner=?,lease_until=? WHERE url=?',
                       (state, error, time.time()+7200, 'original_owner', time.time()+7200, urls[0]))
    protected = dict(old.db.execute('SELECT * FROM articles WHERE url=?', (urls[0],)).fetchone())
    waiting = dict(old.db.execute('SELECT * FROM articles WHERE url=?', (urls[1],)).fetchone())
    header = dict(old.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', (provider,)).fetchone())
    receipt_sha = sha256_file(tmp_path / header['receipt_path'])
    digest = membership_digest(old.db, provider)
    assert waiting['state'] == 'waiting_cohort'
    old.close()
    for _ in range(2):
        queue = NewsQueue(tmp_path, PolicyRegistry(scoped_config(provider), base=Path.cwd()))
        try:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (urls[0],)).fetchone()) == protected
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (urls[1],)).fetchone()) == {**waiting, 'state': 'pending'}
            assert dict(queue.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', (provider,)).fetchone()) == header
            assert membership_digest(queue.db, provider) == digest
            assert sha256_file(tmp_path / header['receipt_path']) == receipt_sha
            assert queue.claim_url(urls[0], 300) is None
        finally:
            queue.close()


def test_cmmedia_scope_transition_does_not_change_body_clock_author_or_parser_repair_identity():
    before = PolicyRegistry(scoped_config('cmmedia', mode='finite'), base=Path.cwd()).sources['cmmedia']
    after = PolicyRegistry(scoped_config('cmmedia'), base=Path.cwd()).sources['cmmedia']
    assert before.data_fingerprint != after.data_fingerprint
    assert before.parser_repair_fingerprint == after.parser_repair_fingerprint
    old = extract_article(cmmedia_page(), before, page_url=CMMEDIA_URL)
    new = extract_article(cmmedia_page(), after, page_url=CMMEDIA_URL)
    for field in ('body', 'body_sha256', 'published_at_raw', 'authors_json', 'extraction_method'):
        assert old[field] == new[field]


@pytest.mark.parametrize('notice', ['本文轉載自《第三方書籍》，非經同意請勿轉載。',
                                    '內容來源：第三方授權轉載', '未經授權不得轉載'])
def test_incremental_cmmedia_keeps_original_third_party_exclusions(notice):
    source = PolicyRegistry(scoped_config('cmmedia'), base=Path.cwd()).sources['cmmedia']
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(cmmedia_page(body='本機原件' * 50 + '</p><blockquote><p>' + notice
                                    + '</p></blockquote><p>'), source, page_url=CMMEDIA_URL)


def test_normal_sources_keep_discovery_and_only_opted_in_sources_get_new_schedule_identity(config):
    original = PolicyRegistry(config, base=Path.cwd()).sources['publisher']
    assert not original.known_url_only
    assert NewsQueue.discovery_dispatch_filter(original)[0] != '0'
    assert NewsQueue.discovery_dispatch_filter(replace(original, known_url_only=True)) == ('0', [])


def test_candidate_has_exactly_two_reviewed_mode_changes_and_priority_source_unchanged():
    cfg, before, repo = load_config(Path('configs/news_fulltext.json'))
    cfg = deepcopy(cfg)
    for entry in cfg['providers']:
        if entry['id'] not in PROVIDERS:
            continue
        entry['acquisition_mode'] = 'continuous'
        entry['private_known_url_scope'] = ('personal_noncommercial_originals_v1' if entry['id'] == 'cmmedia'
                                          else 'per_article_licensed_text_v1')
        if entry['id'] == 'peopo':
            entry['require_configured_container'] = True
    after = PolicyRegistry(cfg, base=repo)
    assert len(before.sources) == len(after.sources) == 66
    for provider in before.sources:
        if provider in PROVIDERS:
            assert after.sources[provider].known_url_only
        else:
            assert before.sources[provider] == after.sources[provider]
