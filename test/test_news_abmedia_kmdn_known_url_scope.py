"""Versioned native scope growth, without discovery, rescans or refusal resets."""
from copy import deepcopy
from dataclasses import replace
import json
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
from test_news_fulltext import FakeResponse, http_fixture
from test_news_taiwan_public_pages import KMDN_URL, TEXT, kmdn_page


COPYRIGHT = 'https://www.kmdn.gov.tw/1119/1121/1367/'
URLS = {'abmedia': ('https://abmedia.io/original-article', 'https://abmedia.io/next-original'),
        'kmdn': (KMDN_URL, 'https://www.kmdn.gov.tw/1117/1271/1277/332174')}
DISCOVERY = ('feeds','sitemaps','wordpress_apis','cnyes_apis','fulltext_feeds','index_pages',
             'incremental_index_pages','metadata_feeds','cnyes_public_index')


def scoped_config(provider, *, finite=False, legacy_parser=False):
    cfg, _, _ = load_config(Path(os.environ.get('STOCKAGENT_NEWS_SCOPE_CONFIG', 'configs/news_fulltext.json')))
    entry = deepcopy(next(p for p in cfg['providers'] if p['id'] == provider))
    entry['acquisition_mode'] = 'finite' if finite else 'continuous'
    for key in DISCOVERY:
        entry.pop(key, None)
    if finite:
        entry.pop('private_known_url_scope', None)
    else:
        entry['private_known_url_scope'] = 'personal_noncommercial_originals_v1'
    if provider == 'kmdn':
        entry['embedded_article_layout'] = 'kmdn_visible_article_v1' if legacy_parser else 'kmdn_visible_article_v2'
        entry['authorization']['evidence_url'] = COPYRIGHT
        entry['article_path_pattern'] = '^/1117/1271/(?:1272|1277|287081)/[1-9][0-9]*/?$'
    return {**cfg, 'providers': [entry]}


def abmedia_page(url, *, author='本篇作者', clock='2014-02-03T08:10:11+08:00', body=TEXT, extra=''):
    data = {'@type':'NewsArticle','headline':'官方本篇新聞','datePublished':clock,
            'author': {'@type':'Person','name':author}, 'mainEntityOfPage':url,'isAccessibleForFree':True}
    return ('<html><head><meta charset="utf-8"><link rel="canonical" href="' + url + '">'
        '<meta property="og:url" content="' + url + '"><script type="application/ld+json">' +
        json.dumps(data, ensure_ascii=False) + '</script></head><body><article class="page-single">'
        '<header><h1>官方本篇新聞</h1><a href="/author/native/">' + author + '</a></header>'
        '<div class="desc"><p>' + body + '</p></div></article>' + extra + '</body></html>').encode()


def page(provider, url, **changes):
    if provider == 'abmedia':
        return abmedia_page(url, **changes)
    return kmdn_page(facebook=url, twitter=url, **changes)


@pytest.mark.parametrize('provider', URLS)
def test_reviewed_local_growth_preserves_owner_scope_and_disables_every_discovery_route(provider):
    source = PolicyRegistry(scoped_config(provider), base=Path.cwd()).sources[provider]
    assert source.allowed and source.known_url_only and source.acquisition_mode == 'continuous'
    assert source.require_configured_container and source.require_author_attribution
    assert not any((source.feeds, source.sitemaps, source.index_pages, source.wordpress_apis,
                    source.cnyes_apis, source.fulltext_feeds, source.metadata_feeds, source.incremental_index_pages))
    if provider == 'kmdn':
        assert source.domains == ('www.kmdn.gov.tw',)
        assert source.embedded_article_layout == 'kmdn_visible_article_v2'
        assert source.authorization['evidence_url'] == COPYRIGHT


@pytest.mark.parametrize('provider', URLS)
@pytest.mark.parametrize('field', DISCOVERY)
def test_known_url_growth_rejects_even_dormant_discovery_configuration(provider, field):
    cfg = scoped_config(provider)
    cfg['providers'][0][field] = ['https://example.test/endpoint']
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('provider', URLS)
@pytest.mark.parametrize('field,value', [
    ('private_known_url_scope', None), ('private_known_url_scope', True), ('private_known_url_scope', 'unreviewed'),
    ('private_known_url_scope','per_article_licensed_text_v1'), ('acquisition_mode','unreviewed'),
    ('require_author_attribution',False), ('article_selectors',[]), ('article_path_pattern',''),
    ('authorization',{'kind':'unknown'}),
])
def test_local_growth_is_explicit_not_an_unreviewed_scope_or_body_grant(provider, field, value):
    cfg = scoped_config(provider)
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('field,value', [
    ('id','other_publisher'),('domains',['other.test']),('require_configured_container',False),
    ('authorization',{'kind':'personal_research','evidence_url':'https://other.test/terms'}),
])
def test_kmdn_v2_cannot_transfer_native_scope_or_copyright_to_another_source(field, value):
    cfg = scoped_config('kmdn')
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('provider', URLS)
@pytest.mark.parametrize('state,error', [
    ('retry','http_429'),('retry_cooldown','http_403'),('robots_disallowed','robots_disallowed'),
    ('paywall_or_login','paywall_or_login'),('source_not_found','source_not_found'),
    ('article_rights_or_scope_excluded','article_rights_or_scope_excluded'),
    ('article_attribution_not_verified','article_attribution_not_verified'),
])
def test_scope_release_and_restart_preserve_refusals_deadlines_attempts_leases_and_frozen_receipts(tmp_path, provider, state, error):
    # Scheduling-only release uses the same body-parser ABI on both sides.
    # The independently tested v1 -> v2 parser upgrade is not a scope-only edit.
    old_url, new_url = URLS[provider]
    old = NewsQueue(tmp_path, PolicyRegistry(scoped_config(provider, finite=True), base=Path.cwd()))
    with old.db:
        old.add_articles([{'url':old_url}])
    old.sync_finite_cohorts()
    with old.db:
        old.add_articles([{'url':new_url}])
        old.db.execute('UPDATE articles SET state=?,error_code=?,due=?,attempts=4,lease_owner=?,lease_until=? WHERE url=?',
            (state,error,time.time()+7200,'original_owner',time.time()+7200,old_url))
        old.db.execute('UPDATE articles SET due=?,attempts=2,error_code=? WHERE url=?',
            (time.time()+7200,'old_scope_witness',new_url))
    protected = dict(old.db.execute('SELECT * FROM articles WHERE url=?',(old_url,)).fetchone())
    waiting = dict(old.db.execute('SELECT * FROM articles WHERE url=?',(new_url,)).fetchone())
    header = dict(old.db.execute('SELECT * FROM finite_cohorts').fetchone())
    digest, receipt_sha = membership_digest(old.db,provider), sha256_file(tmp_path/header['receipt_path'])
    old.close()
    for _ in range(2):
        queue = NewsQueue(tmp_path, PolicyRegistry(scoped_config(provider), base=Path.cwd()))
        try:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(old_url,)).fetchone()) == protected
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(new_url,)).fetchone()) == {**waiting,'state':'pending'}
            assert dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone()) == header
            assert sha256_file(tmp_path/header['receipt_path']) == receipt_sha
            assert membership_digest(queue.db,provider) == digest
            assert queue.claim_url(old_url,300) is None and queue.claim_url(new_url,300) is None
        finally:
            queue.close()


@pytest.mark.parametrize('provider', URLS)
def test_finite_annotation_cannot_extend_original_membership(tmp_path, provider):
    cfg = scoped_config(provider, finite=True)
    cfg['providers'][0]['private_known_url_scope'] = 'personal_noncommercial_originals_v1'
    queue = NewsQueue(tmp_path,PolicyRegistry(cfg,base=Path.cwd()))
    try:
        old_url,new_url = URLS[provider]
        with queue.db:
            queue.add_articles([{'url':old_url}])
        queue.sync_finite_cohorts()
        with queue.db:
            queue.add_articles([{'url':new_url}])
        assert not queue.registry.sources[provider].known_url_only
        assert queue.db.execute('SELECT state FROM articles WHERE url=?',(new_url,)).fetchone()[0] == 'waiting_cohort'
    finally:
        queue.close()


@pytest.mark.parametrize('provider', URLS)
@pytest.mark.parametrize('backend',['html.parser','lxml'])
def test_current_original_body_author_and_source_clock_unchanged_in_both_parsers(provider,backend):
    old = PolicyRegistry(scoped_config(provider,finite=True,legacy_parser=True),base=Path.cwd()).sources[provider]
    new = PolicyRegistry(scoped_config(provider),base=Path.cwd()).sources[provider]
    raw,url = page(provider,URLS[provider][0]),URLS[provider][0]
    a,b = [extract_article(raw,replace(s,html_parser=backend),page_url=url) for s in (old,new)]
    assert old.data_fingerprint != new.data_fingerprint
    if provider == 'abmedia':
        assert old.parser_repair_fingerprint == new.parser_repair_fingerprint
        assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    else:
        assert old.parser_repair_fingerprint != new.parser_repair_fingerprint
    for key in ('body','body_sha256','title','published_at_raw','authors_json','published_time_basis','author_attribution_detail_json'):
        assert a.get(key) == b.get(key)


@pytest.mark.parametrize('category',['1272','1277','287081'])
@pytest.mark.parametrize('backend',['html.parser','lxml'])
def test_kmdn_reviewed_v2_namespace_keeps_exact_native_author_and_date_only_clock(category,backend):
    url = f'https://www.kmdn.gov.tw/1117/1271/{category}/332174'
    source = PolicyRegistry(scoped_config('kmdn'),base=Path.cwd()).sources['kmdn']
    result = extract_article(kmdn_page(facebook=url,twitter=url,label='作者：',byline='本篇投稿作者。'),
                             replace(source,html_parser=backend),page_url=url)
    assert json.loads(result['authors_json']) == ['本篇投稿作者。']
    assert result['published_at_raw'] == '2019/05/26'
    assert result['published_time_basis'] == 'publisher_declared_date_only; no_intraday_time_inferred'


@pytest.mark.parametrize('category',['1273','1276','9999'])
def test_kmdn_v2_does_not_expand_to_unreviewed_categories(category):
    source = PolicyRegistry(scoped_config('kmdn'),base=Path.cwd()).sources['kmdn']
    url = f'https://www.kmdn.gov.tw/1117/1271/{category}/332174'
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(kmdn_page(facebook=url,twitter=url),source,page_url=url)


def test_kmdn_v1_remains_incapable_of_accepting_student_namespace():
    source = PolicyRegistry(scoped_config('kmdn',finite=True,legacy_parser=True),base=Path.cwd()).sources['kmdn']
    url = URLS['kmdn'][1]
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(kmdn_page(facebook=url,twitter=url),source,page_url=url)


@pytest.mark.parametrize('changes,code', [
    ({'twitter':KMDN_URL},'article_identity_not_verified'),
    ({'byline':''},'article_attribution_not_verified'),
    ({'clock':'2019/02/30'},'invalid_bulk_publication_clock'),
    ({'extra':'<form action="/login"><input type="password"></form>'},'paywall_or_login'),
])
def test_student_article_still_requires_both_current_share_urls_byline_date_and_no_access_gate(changes,code):
    url = URLS['kmdn'][1]
    source = PolicyRegistry(scoped_config('kmdn'),base=Path.cwd()).sources['kmdn']
    with pytest.raises(NewsError,match=code):
        extract_article(kmdn_page(**{'facebook':url,'twitter':url,**changes}),source,page_url=url)


@pytest.mark.parametrize('provider',URLS)
def test_formal_chain_downloads_only_new_local_head_no_old_rescan_or_dormant_index_dispatch(tmp_path,provider):
    old_url,new_url = URLS[provider]
    oldcfg = scoped_config(provider,finite=True,legacy_parser=True)
    robots = 'https://' + ('abmedia.io' if provider == 'abmedia' else 'www.kmdn.gov.tw') + '/robots.txt'
    http,session = http_fixture(oldcfg,tmp_path,{robots:FakeResponse(b'User-agent: *\nDisallow:\n'),old_url:FakeResponse(page(provider,old_url))})
    queue = NewsQueue(tmp_path,http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url':old_url}])
        queue.sync_finite_cohorts()
        run_cycle(queue,http,oldcfg,max_articles=1,max_discovery_requests=0)
        with queue.db:
            queue.add_articles([{'url':new_url}])
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,0)',(robots.replace('/robots.txt','/'),'index',provider))
        head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(old_url,)).fetchone())
        assert head['state'] == 'complete'
        header = dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())
        receipt_sha = sha256_file(tmp_path/header['receipt_path'])
        raw_sha = sha256_file(tmp_path/head['parquet_path'])
    finally:
        http.close()
        queue.close()
    cfg = scoped_config(provider)
    http,session = http_fixture(cfg,tmp_path,{robots:FakeResponse(b'User-agent: *\nDisallow:\n'),new_url:FakeResponse(page(provider,new_url))})
    queue = NewsQueue(tmp_path,http.registry)
    try:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(old_url,)).fetchone()) == head
        seed_discovery(queue)
        assert queue.discovery_candidates(http.registry.sources[provider],10) == []
        assert run_cycle(queue,http,cfg,max_articles=1,max_discovery_requests=0)['providers'][provider]['states']['complete'] == 2
        assert old_url not in [url for url,_ in session.calls]
        assert sha256_file(tmp_path/head['parquet_path']) == raw_sha
        assert sha256_file(tmp_path/header['receipt_path']) == receipt_sha
        record = next(r for r in pq.read_table(tmp_path/queue.db.execute('SELECT parquet_path FROM articles WHERE url=?',(new_url,)).fetchone()[0]).to_pylist()
                      if r['requested_url'] == new_url)
        assert record['data_fingerprint'] == http.registry.sources[provider].data_fingerprint
        assert verify_bodies(tmp_path)['accepted']
        proof = audit(tmp_path,[old_url,new_url],registry=http.registry)
        assert proof['accepted'],proof['errors']
    finally:
        http.close()
        queue.close()
