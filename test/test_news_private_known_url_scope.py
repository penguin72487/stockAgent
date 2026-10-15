"""Known private-use scope growth is distinct from body/access acceptance."""
from copy import deepcopy
import json
import os
from pathlib import Path
import time

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.audit_news_finite_cohorts import audit_cohorts
from stockagent.data.news_source_catalog import load_news_configuration
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


@pytest.mark.parametrize('provider,evidence,layout', [
    ('moneyweekly', 'https://www.moneyweekly.com.tw/ArticleData/Info/%E7%BD%AE%E5%BA%95%E6%96%87%E7%AB%A0/7', ''),
    ('nownews', 'https://www.nownews.com/copyright', 'nownews_visible_article_v1'),
])
def test_explicit_personal_known_url_policy_keeps_original_content_boundaries(provider, evidence, layout):
    path = Path(os.environ.get('STOCKAGENT_NEWS_SCOPE_CONFIG', 'configs/news_fulltext.json'))
    cfg, registry, _ = load_config(path)
    source = registry.sources[provider]
    entry = next(p for p in cfg['providers'] if p['id'] == provider)
    assert cfg['collection_purpose'] == 'personal_research'
    assert cfg['exclusive_http_owner'] is True
    assert source.allowed and source.acquisition_mode == 'continuous'
    assert source.authorization['kind'] == 'personal_research'
    assert source.authorization['evidence_url'] == evidence
    assert source.authorization['reviewed_on'] == '2026-10-14'
    assert entry['private_known_url_scope'] == 'personal_noncommercial_originals_v1'
    assert 'locally known public original' in source.authorization['scope']
    assert 'third-party' in source.authorization['scope']
    assert 'training' in source.authorization['scope']
    assert source.embedded_article_layout == layout
    assert source.require_author_attribution
    assert not any((source.feeds, source.sitemaps, source.wordpress_apis, source.cnyes_apis,
                    source.index_pages, source.incremental_index_pages, source.fulltext_feeds))
    assert source.interval_seconds >= 2 and source.parallelism == 1
    if provider == 'moneyweekly':
        assert source.permitted_author_names == ('理周投研部', '洪崇晏')
        assert source.selectors == ('.div_Article_Info[itemprop=articleBody]',)
    else:
        assert source.require_configured_container
        assert source.domains == ('www.nownews.com',)
        assert source.selectors == ('#articleContent.article-content-edtor', '.newsMsg')


@pytest.mark.parametrize('provider', ['ftvnews', 'genetinfo', 'inside', 'pansci', 'cmmedia', 'cnews'])
def test_each_private_source_retains_its_explicit_admission_mode(provider):
    _, registry, _ = load_config(Path(os.environ.get('STOCKAGENT_NEWS_SCOPE_CONFIG', 'configs/news_fulltext.json')))
    expected = 'continuous' if provider in {'ftvnews', 'genetinfo'} or (
        provider in {'cmmedia', 'cnews'} and registry.sources[provider].known_url_only) else 'finite'
    assert registry.sources[provider].acquisition_mode == expected
    if provider == 'inside':
        assert not registry.sources[provider].allowed


@pytest.mark.parametrize('field,value', [
    ('id', 'other_publisher'), ('domains', ['unknown.test']),
    ('authorization', {'kind': 'personal_research', 'evidence_url': 'https://unknown.test/copyright'}),
    ('require_configured_container', False), ('require_author_attribution', False),
    ('embedded_article_layout', 'unreviewed_layout'), ('acquisition_mode', 'unreviewed_mode'),
    ('private_known_url_scope', None), ('private_known_url_scope', 'unreviewed_scope'),
])
def test_continuous_nownews_mode_does_not_relax_registered_source_guards(field, value):
    path = Path(os.environ.get('STOCKAGENT_NEWS_SCOPE_CONFIG', 'configs/news_fulltext.json'))
    cfg = load_news_configuration(path)
    entry = deepcopy(next(p for p in cfg['providers'] if p['id'] == 'nownews'))
    entry['acquisition_mode'] = 'continuous'
    entry['private_known_url_scope'] = 'personal_noncommercial_originals_v1'
    entry[field] = value
    with pytest.raises(ValueError):
        PolicyRegistry({**cfg, 'providers': [entry]}, base=Path.cwd())


@pytest.mark.parametrize('state,error', [
    ('robots_disallowed', 'robots_disallowed'), ('paywall_or_login', 'paywall_or_login'),
    ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded'),
    ('article_identity_not_verified', 'article_identity_not_verified'),
    ('source_not_found', 'source_not_found'), ('source_article_empty_template', 'source_article_empty_template'),
    ('retry', 'http_429'), ('retry_cooldown', 'http_403'),
])
def test_known_url_scope_releases_only_local_gate_preserves_refusal_and_future_due(config, tmp_path, state, error):
    finite = deepcopy(config)
    finite['providers'][0]['acquisition_mode'] = 'finite'
    old = NewsQueue(tmp_path, PolicyRegistry(finite, base=tmp_path))
    with old.db:
        old.add_articles([{'url': 'https://publisher.test/old'}])
    old.sync_finite_cohorts()
    with old.db:
        old.add_articles([{'url': 'https://publisher.test/new', 'title': '原始標題', 'date': '2014-01-02'}])
        old.db.execute("UPDATE articles SET state=?,error_code=?,due=?,attempts=3,last_checked_at_utc=? "
                       "WHERE url='https://publisher.test/old'",
                       (state, error, time.time() + 3600, '2026-10-14T04:00:00+00:00'))
    header = dict(old.db.execute('SELECT * FROM finite_cohorts').fetchone())
    original_digest = sha256_file(tmp_path / header['receipt_path'])
    protected = dict(old.db.execute("SELECT * FROM articles WHERE url='https://publisher.test/old'").fetchone())
    waiting = dict(old.db.execute("SELECT * FROM articles WHERE url='https://publisher.test/new'").fetchone())
    assert waiting['state'] == 'waiting_cohort'
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert dict(new.db.execute("SELECT * FROM articles WHERE url='https://publisher.test/old'").fetchone()) == protected
        ready = dict(new.db.execute("SELECT * FROM articles WHERE url='https://publisher.test/new'").fetchone())
        assert ready == {**waiting, 'state': 'pending'}
        assert dict(new.db.execute('SELECT * FROM finite_cohorts').fetchone()) == header
        assert sha256_file(tmp_path / header['receipt_path']) == original_digest
        assert new.claim_url('https://publisher.test/old', 300) is None
        assert new.claim_url('https://publisher.test/new', 300)['url'] == waiting['url']
    finally:
        new.close()


def test_scope_growth_preserves_old_body_and_only_new_body_uses_current_fingerprint(config, tmp_path):
    finite = deepcopy(config)
    finite['providers'][0]['acquisition_mode'] = 'finite'
    http, session = http_fixture(finite, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nDisallow:\n'),
        'https://publisher.test/old': FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    with queue.db:
        queue.add_articles([{'url': 'https://publisher.test/old'}])
    run_cycle(queue, http, finite, max_articles=1, max_discovery_requests=0)
    original = dict(queue.db.execute("SELECT * FROM articles WHERE url='https://publisher.test/old'").fetchone())
    raw_path = tmp_path / original['parquet_path']
    original_raw_hash = sha256_file(raw_path)
    old_fingerprint = http.registry.sources['publisher'].data_fingerprint
    with queue.db:
        queue.add_articles([{'url': 'https://publisher.test/new'}])
    http.close()
    queue.close()
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nDisallow:\n'),
        'https://publisher.test/new': FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        assert dict(queue.db.execute("SELECT * FROM articles WHERE url='https://publisher.test/old'").fetchone()) == original
        assert http.registry.sources['publisher'].data_fingerprint != old_fingerprint
        assert sha256_file(raw_path) == original_raw_hash
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert result['providers']['publisher']['states']['complete'] == 2
        assert not result['providers']['publisher'].get('finite_cohort')
        assert len([url for url, _ in session.calls if url.endswith('/new')]) == 1
        assert not any(url.endswith('/old') for url, _ in session.calls)
        assert sha256_file(raw_path) == original_raw_hash
        assert verify_bodies(tmp_path)['accepted']
        assert audit_cohorts(tmp_path)['scope_integrity_accepted']
    finally:
        http.close()
        queue.close()
