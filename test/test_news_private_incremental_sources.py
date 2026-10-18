"""Incremental private URL admission does not relax source observations."""
from copy import deepcopy
import json
import os
from pathlib import Path
import time

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_cohorts import materialize_reviewed_supplements, membership_digest
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from downloader.artifact_io import sha256_file
from test_news_public_originals import CLOCK, FTV_URL, TEXT, ftv_page


PROVIDERS = ('ftvnews', 'genetinfo')


def configuration():
    path = Path(os.environ.get('STOCKAGENT_NEWS_INCREMENTAL_SCOPE_CONFIG', 'configs/news_fulltext.json'))
    return load_config(path)[0]


def single_config(provider, *, mode='continuous'):
    cfg = configuration()
    entry = deepcopy(next(p for p in cfg['providers'] if p['id'] == provider))
    entry['acquisition_mode'] = mode
    if mode == 'continuous':
        entry['private_known_url_scope'] = 'personal_noncommercial_originals_v1'
    return {**cfg, 'providers': [entry]}


@pytest.mark.parametrize('provider', PROVIDERS)
def test_explicit_incremental_sources_retain_original_observation_guards(provider):
    cfg = configuration()
    entry = next(p for p in cfg['providers'] if p['id'] == provider)
    source = PolicyRegistry(cfg, base=Path.cwd()).sources[provider]
    assert source.allowed and source.acquisition_mode == 'continuous'
    assert entry['private_known_url_scope'] == 'personal_noncommercial_originals_v1'
    assert source.allow_reviewed_supplements  # Historical descriptors, not new admission.
    assert source.authorization['kind'] == 'personal_research'
    assert source.authorization['reviewed_on'] == '2026-10-14'
    assert source.require_configured_container and source.require_author_attribution
    assert source.interval_seconds >= 2 and source.parallelism == 1
    assert not any((source.feeds, source.sitemaps, source.wordpress_apis, source.cnyes_apis,
                    source.index_pages, source.incremental_index_pages, source.fulltext_feeds))
    if provider == 'ftvnews':
        assert source.embedded_article_layout == 'ftv_visible_article_v7'
        assert source.selectors == ('#contentarea[itemprop=articleBody]',)
    else:
        assert source.selectors == ('.post-content-single > .post-item > .post-content-details',)
        assert source.author_selectors == ('.post-content-single > .post-item > .post-info > .post-autor',)


@pytest.mark.parametrize('provider', PROVIDERS)
@pytest.mark.parametrize('field,value', [
    ('private_known_url_scope', None), ('private_known_url_scope', 'unreviewed_scope'),
    ('acquisition_mode', 'unreviewed_mode'), ('domains', ['unknown.test']),
    ('require_configured_container', False), ('require_author_attribution', False),
    ('authorization', {'kind': 'personal_research', 'evidence_url': 'https://unknown.test/copyright'}),
    ('allow_reviewed_supplements', 'true'),
])
def test_retaining_historical_supplements_does_not_grant_generic_scope(provider, field, value):
    cfg = single_config(provider)
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('provider', PROVIDERS)
def test_legacy_finite_source_and_parser_repair_identity_are_retained(provider):
    old = single_config(provider, mode='finite')
    old['providers'][0].pop('private_known_url_scope', None)
    before = PolicyRegistry(old, base=Path.cwd()).sources[provider]
    after = PolicyRegistry(single_config(provider), base=Path.cwd()).sources[provider]
    assert before.acquisition_mode == 'finite' and before.allow_reviewed_supplements
    assert after.allow_reviewed_supplements and after.acquisition_mode == 'continuous'
    assert before.data_fingerprint != after.data_fingerprint
    assert before.parser_repair_fingerprint == after.parser_repair_fingerprint


@pytest.mark.parametrize('provider', PROVIDERS)
def test_retained_flag_cannot_materialize_new_supplement_in_continuous_mode(provider, tmp_path):
    registry = PolicyRegistry(single_config(provider), base=Path.cwd())
    queue = NewsQueue(tmp_path, registry)
    url = FTV_URL if provider == 'ftvnews' else 'https://www.genetinfo.com/investment/featured/item/123.html'
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        with pytest.raises(ValueError, match='source has not enabled reviewed supplemental capture'):
            materialize_reviewed_supplements(queue, [url], lease_seconds=300)
        assert queue.db.execute('SELECT count(*) FROM finite_capture_supplements').fetchone()[0] == 0
        assert queue.db.execute('SELECT attempts FROM articles WHERE url=?', (url,)).fetchone()[0] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('provider', PROVIDERS)
@pytest.mark.parametrize('state,error', [('robots_disallowed','robots_disallowed'),
    ('article_rights_or_scope_excluded','article_rights_or_scope_excluded'),
    ('paywall_or_login','paywall_or_login'), ('retry','http_429'), ('retry_cooldown','http_403')])
def test_only_local_cohort_wait_is_released_with_original_receipts_and_leases_intact(provider, state, error, tmp_path):
    old_cfg = single_config(provider, mode='finite')
    old_cfg['providers'][0].pop('private_known_url_scope', None)
    urls = ([FTV_URL, 'https://www.ftvnews.com.tw/news/detail/2026317W0045'] if provider == 'ftvnews'
            else ['https://www.genetinfo.com/investment/featured/item/123.html',
                  'https://www.genetinfo.com/investment/featured/item/124.html'])
    old = NewsQueue(tmp_path, PolicyRegistry(old_cfg, base=Path.cwd()))
    with old.db:
        old.add_articles([{'url': urls[0]}])
    old.sync_finite_cohorts()
    with old.db:
        old.add_articles([{'url': urls[1], 'date': '2014-01-02', 'title': '實際本機舊標題'}])
        old.db.execute('UPDATE articles SET state=?,error_code=?,due=?,attempts=3,lease_owner=?,lease_until=? WHERE url=?',
                       (state, error, time.time()+3600, 'preserved', time.time()+3600, urls[0]))
    original = dict(old.db.execute('SELECT * FROM articles WHERE url=?', (urls[0],)).fetchone())
    waiting = dict(old.db.execute('SELECT * FROM articles WHERE url=?', (urls[1],)).fetchone())
    header = dict(old.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', (provider,)).fetchone())
    receipt_sha = sha256_file(tmp_path / header['receipt_path'])
    membership = membership_digest(old.db, provider)
    assert waiting['state'] == 'waiting_cohort'
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(single_config(provider), base=Path.cwd()))
    try:
        assert dict(new.db.execute('SELECT * FROM articles WHERE url=?', (urls[0],)).fetchone()) == original
        assert dict(new.db.execute('SELECT * FROM articles WHERE url=?', (urls[1],)).fetchone()) == {**waiting, 'state':'pending'}
        assert dict(new.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', (provider,)).fetchone()) == header
        assert membership_digest(new.db, provider) == membership
        assert sha256_file(tmp_path / header['receipt_path']) == receipt_sha
        assert new.claim_url(urls[0], 300) is None
        assert new.claim_url(urls[1], 300)['url'] == urls[1]
    finally:
        new.close()


@pytest.mark.parametrize('mode', ['finite','continuous'])
def test_ftv_original_body_and_declared_clock_parity_across_scope_only_change(mode):
    source = PolicyRegistry(single_config('ftvnews', mode=mode), base=Path.cwd()).sources['ftvnews']
    original = extract_article(ftv_page(), source, page_url=FTV_URL)
    assert TEXT in original['body'] and '風險提醒不能刪除' in original['body']
    assert original['published_at_raw'] == CLOCK
    assert json.loads(original['authors_json']) == ['梁博超（即時中心） - 民視新聞網']
    assert '圖片說明不可作正文' not in original['body']


@pytest.mark.parametrize('credit', ['（中央社）', 'CNEWS／外部作者', '圖文／CTWANT'])
def test_ftv_incremental_scope_never_turns_syndicated_text_into_newsroom_originals(credit):
    source = PolicyRegistry(single_config('ftvnews'), base=Path.cwd()).sources['ftvnews']
    payload = ftv_page().replace(b'<p class="aphorism">', f'<p>{credit}</p><p class="aphorism">'.encode())
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(payload, source, page_url=FTV_URL)


def test_genet_visible_external_author_and_date_only_clock_are_not_relabelled_or_backdated():
    source = PolicyRegistry(single_config('genetinfo'), base=Path.cwd()).sources['genetinfo']
    text = '原始數值 -12.5 與外部署名不可以改成自有記者。' * 20
    payload = f'''<div class="post-content-single"><div class="post-item"><div class="post-title"><h2>本篇標題</h2></div>
        <div class="post-info"><span class="post-date">日期 : 2020/3/2</span><span class="post-autor">作者 : 實際來源作者</span></div>
        <div class="post-content-details"><p>{text}</p></div></div></div>'''.encode()
    article = extract_article(payload, source, page_url='https://www.genetinfo.com/investment/featured/item/123.html')
    assert article['body'] == text
    assert json.loads(article['authors_json']) == ['作者 : 實際來源作者']
    assert article['published_at_raw'] == '日期 : 2020/3/2'
    assert article['license_verification_basis'] == 'article_declared_license_or_source_policy'
    assert article['per_article_license_verified'] is False
    assert json.loads(article['declared_license_urls_json']) == []
