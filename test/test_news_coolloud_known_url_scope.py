"""Reviewed publisher-original scope growth cannot transfer a site license."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import time

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_cohorts import membership_digest
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, http_fixture
from test_news_coolloud_native import COPYRIGHT, URL, configured, page


SCOPE = 'publisher_original_open_license_v1'
NEW_URL = 'https://www.coolloud.org.tw/node/61644'


def known_config():
    cfg = configured()
    cfg['providers'][0].update(acquisition_mode='continuous', private_known_url_scope=SCOPE)
    return cfg


def test_known_original_growth_has_its_own_source_scope_not_a_per_article_cc_claim():
    old = PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud']
    new = PolicyRegistry(known_config(), base=Path.cwd()).sources['coolloud']
    assert new.allowed and new.known_url_only and new.acquisition_mode == 'continuous'
    assert old.parser_repair_fingerprint == new.parser_repair_fingerprint
    assert old.data_fingerprint != new.data_fingerprint
    assert new.authorization == old.authorization
    assert not new.required_license_url_prefix
    assert not any((new.feeds, new.sitemaps, new.wordpress_apis, new.cnyes_apis,
                    new.index_pages, new.incremental_index_pages, new.metadata_feeds, new.fulltext_feeds))


@pytest.mark.parametrize('field,value', [
    ('id', 'other'), ('domains', ['coolloud.org.tw']), ('domains', ['other.test']),
    ('embedded_article_layout', 'unreviewed'), ('article_selectors', ['article']),
    ('require_configured_container', False), ('require_author_attribution', False),
    ('private_known_url_scope', 'per_article_licensed_text_v1'),
    ('private_known_url_scope', 'personal_noncommercial_originals_v1'),
    ('private_known_url_scope', 'unreviewed'), ('acquisition_mode', 'unreviewed'),
])
def test_known_original_scope_requires_exact_reviewed_provider_native_recipe(field, value):
    cfg = known_config()
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('field,value', [
    ('kind', 'personal_research'), ('kind', 'public_domain'), ('license', 'CC-BY-4.0'),
    ('evidence_url', 'https://www.coolloud.org.tw/'),
    ('evidence_url', 'https://other.test/node/4524'),
])
def test_linked_site_notice_cannot_be_replaced_by_a_different_grant(field, value):
    cfg = known_config()
    cfg['providers'][0]['authorization'][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('field', ['feeds', 'sitemaps', 'wordpress_apis', 'cnyes_apis',
    'fulltext_feeds', 'index_pages', 'incremental_index_pages', 'metadata_feeds', 'cnyes_public_index'])
def test_known_original_capture_cannot_enable_dormant_or_new_discovery(field):
    cfg = known_config()
    cfg['providers'][0][field] = ['https://www.coolloud.org.tw/unreviewed']
    # Bulk/feed policy rejects these endpoints even before known-URL admission.
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
@pytest.mark.parametrize('changes,code', [
    ({'kind': 'news', 'source': '鉅亨網'}, 'article_rights_or_scope_excluded'),
    ({'kind': 'outer', 'source': '外部投稿'}, 'article_rights_or_scope_excluded'),
    ({'source': '第三方'}, 'article_rights_or_scope_excluded'),
    ({'staff': '外部作者'}, 'article_attribution_not_verified'),
    ({'published': '2019/09/16'}, 'invalid_bulk_publication_clock'),
    ({'extra': '<div class="paywall">限制内容</div>'}, 'paywall_or_login'),
])
def test_incremental_scope_never_relaxes_body_identity_clock_attribution_or_access(backend, changes, code):
    old = PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud']
    new = PolicyRegistry(known_config(), base=Path.cwd()).sources['coolloud']
    for source in (old, new):
        with pytest.raises(NewsError, match=code):
            extract_article(page(**changes), replace(source, html_parser=backend), page_url=URL)


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
def test_same_native_original_is_exactly_unchanged_by_the_schedule_migration(backend):
    old = PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud']
    new = PolicyRegistry(known_config(), base=Path.cwd()).sources['coolloud']
    a, b = [extract_article(page(), replace(source, html_parser=backend), page_url=URL) for source in (old, new)]
    assert a == b
    assert not b['per_article_license_verified']
    assert b['license_verification_basis'] == 'publisher_original_bound_to_linked_CC-BY-NC-SA-2.0-TW_notice_v1'


def test_known_original_growth_preserves_other_sources_and_priority_configuration():
    cfg, old, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(cfg)
    candidate.pop('provider_catalog', None)
    source = next(p for p in candidate['providers'] if p['id'] == 'coolloud')
    source.update(acquisition_mode='continuous', private_known_url_scope=SCOPE)
    new = PolicyRegistry(candidate, base=repo)
    assert old.sources.keys() == new.sources.keys()
    assert all(old.sources[p] == new.sources[p] for p in old.sources if p != 'coolloud')
    assert old.sources['coolloud'].parser_repair_fingerprint == new.sources['coolloud'].parser_repair_fingerprint


def test_restart_uses_only_new_locally_observed_work_and_keeps_frozen_cohort_and_denials(tmp_path):
    old_cfg, cfg = configured(), known_config()
    robots = 'https://www.coolloud.org.tw/robots.txt'
    external = 'https://www.coolloud.org.tw/node/36836'
    denied = 'https://www.coolloud.org.tw/node/97779'
    http, session = http_fixture(old_cfg, tmp_path, {robots: FakeResponse(b'User-agent: *\nDisallow:\n'),
        URL: FakeResponse(page()), external: FakeResponse(page(url=external, node='36836', kind='news'))})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}, {'url': external}])
        queue.sync_finite_cohorts()
        run_cycle(queue, http, old_cfg, max_articles=2, max_discovery_requests=0)
        with queue.db:
            queue.add_articles([{'url': NEW_URL}, {'url': denied}])
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=? WHERE url=?",
                             (time.time() + 3600, denied))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        cohort = dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())
        digest = membership_digest(queue.db, 'coolloud')
        receipt_sha = sha256_file(tmp_path / cohort['receipt_path'])
        assert before[NEW_URL]['state'] == 'waiting_cohort'
    finally:
        http.close()
        queue.close()
    http, session = http_fixture(cfg, tmp_path, {robots: FakeResponse(b'User-agent: *\nDisallow:\n'),
        NEW_URL: FakeResponse(page(url=NEW_URL, node='61644'))})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.reconcile_policy()
        run_cycle(queue, http, cfg, max_articles=3, max_discovery_requests=0)
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after[NEW_URL]['state'] == 'complete' and after[NEW_URL]['body_sha256']
        assert after[URL] == before[URL] and after[external] == before[external]
        assert after[denied] == before[denied]
        assert membership_digest(queue.db, 'coolloud') == digest
        assert sha256_file(tmp_path / cohort['receipt_path']) == receipt_sha
        requests = len(session.calls)
        queue.reconcile_policy()
        queue.sync_finite_cohorts()
        run_cycle(queue, http, cfg, max_articles=3, max_discovery_requests=0)
        assert len(session.calls) == requests
        assert all(url not in {URL, external, denied, COPYRIGHT} for url, _ in session.calls)
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()
