"""Actual Drupal layout contract; synthetic prose, no private fixture needed."""
from dataclasses import replace
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import load_config, run_cycle
from downloader.news_fulltext_cohorts import membership_digest
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, config as base_config, http_fixture


URL = 'https://www.coolloud.org.tw/node/93414'
COPYRIGHT = 'https://www.coolloud.org.tw/node/4524'
TEXT = '本篇可見完整報導包含原始數值 -12.5 與 12345 股，不改寫或補造來源。' * 5
PUBLISHED = '2019-09-16T16:30:42+08:00'
MODIFIED = '2019-09-16T16:51:05+08:00'
SELECTOR = ':scope > .field-name-body > .field-items > .field-item'


def configured():
    cfg = base_config.__wrapped__()  # Reuse the underlying synthetic config builder, not pytest's fixture wrapper.
    cfg['providers'] = [{'id': 'coolloud', 'name': '苦勞網', 'domains': ['www.coolloud.org.tw'],
        'enabled': True, 'acquisition_mode': 'finite', 'interval_seconds': 0.001, 'parallelism': 1,
        'article_path_pattern': '^/node/[1-9][0-9]*$', 'embedded_article_layout': 'coolloud_visible_original_v1',
        'article_selectors': [SELECTOR], 'title_selector': 'h1.page-header',
        'require_configured_container': True, 'require_author_attribution': True,
        'exclude_selectors': ['figure', 'figcaption'],
        'authorization': {'kind': 'open_license', 'license': 'CC-BY-NC-SA-2.0-TW',
                          'evidence_url': COPYRIGHT, 'scope': 'Finite private original-only test contract'}}]
    return cfg


def page(*, url=URL, node='93414', kind='post', source='苦勞報導', source_href='/story',
         reporter='本篇記者', profile='/author/%E6%9C%AC%E7%AF%87%E8%A8%98%E8%80%85', staff='苦勞網記者',
         published=PUBLISHED, modified=MODIFIED, day='2019/09/16', extra='', body=TEXT):
    return f'''<html><head><meta charset="utf-8"><title>本篇新聞 | 苦勞網</title>
    <link rel="canonical" href="{url}"><link rel="shortlink" href="{url}">
    <meta property="og:url" content="{url}"><meta property="og:title" content="本篇新聞">
    <meta property="article:published_time" content="{published}">
    <meta property="article:modified_time" content="{modified}">
    <meta property="og:updated_time" content="{modified}">
    <meta property="article:author" content="https://www.facebook.com/coolloud">
    </head><body><div class="main-container"><div class="row"><section class="col-sm-12">
    <h1 class="page-header"><p>本篇新聞</p></h1><div class="region region-content">
    <article id="node-{node}" class="node node-{kind}"><div class="group-info">
    <div class="field-name-field-actdate"><span class="date-display-single">{day}</span></div>
    <div class="field-name-field-source"><div class="field-items"><div class="field-item">
    <a href="{source_href}">{source}</a></div></div></div></div><div class="group-author">
    <div class="field-name-field-author"><div class="field-items"><div class="field-item">
    <a href="{profile}">{reporter}</a></div></div></div><div class="field-name-field-writernote">
    <div class="field-items"><div class="field-item">{staff}</div></div></div></div>
    <div class="field-name-body"><div class="field-items"><div class="field-item">
    <p>{body}</p><figure><figcaption>媒體圖說不是正文</figcaption><img src="https://external.test/media.jpg"></figure>
    </div></div></div><div class="field-name-field-chiefeditor"><div class="field-label">責任主編:&nbsp;</div>
    <div class="field-items"><div class="field-item"><a href="/chiefeditor/%E6%9C%AC%E7%AF%87%E7%B7%A8%E8%BC%AF">本篇編輯</a>
    </div></div></div></article><aside>相關新聞 {TEXT}</aside></div></section></div></div>
    <footer><a href="/node/4524">版權宣告</a><time>2099-01-01</time><a rel="author">頁尾作者</a></footer>
    <script type="application/ld+json">{json.dumps({'@type':'NewsArticle','articleBody':'隱藏內容'+TEXT,'author':{'name':'網站帳號'}},ensure_ascii=False)}</script>
    {extra}</body></html>'''.encode()


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
def test_actual_malformed_heading_scope_original_reporter_and_exact_source_clocks(backend):
    source = PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud']
    result = extract_article(page(), replace(source, html_parser=backend), page_url=URL)
    assert result['body'] == TEXT and result['title'] == '本篇新聞'
    assert json.loads(result['authors_json']) == ['本篇記者']
    details = json.loads(result['author_attribution_detail_json'])
    assert details['chief_editor_raw'] == '本篇編輯' and details['reporter_raw'] == '本篇記者'
    assert not details['calendar_field_used_as_publication'] and not details['editor_or_facebook_account_used_as_reporter']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert result['license_verification_basis'] == 'publisher_original_bound_to_linked_CC-BY-NC-SA-2.0-TW_notice_v1'
    assert json.loads(result['verified_article_notices_json'])[0]['source_links'] == [COPYRIGHT]
    assert '隱藏內容' not in result['body'] and '媒體圖說' not in result['body']


@pytest.mark.parametrize('changes,code', [
    ({'kind':'news','source':'工商時報'}, 'article_rights_or_scope_excluded'),
    ({'kind':'outer','source':'台灣電信工會'}, 'article_rights_or_scope_excluded'),
    ({'source':'鉅亨網'}, 'article_rights_or_scope_excluded'),
    ({'source_href':'/syndicated'}, 'article_rights_or_scope_excluded'),
    ({'staff':'外部投稿'}, 'article_attribution_not_verified'),
    ({'reporter':''}, 'article_attribution_not_verified'),
    ({'profile':'/author/other'}, 'article_attribution_not_verified'),
    ({'node':'93415'}, 'article_identity_not_verified'),
    ({'url':'https://www.coolloud.org.tw/node/93415'}, 'article_identity_not_verified'),
    ({'published':'2019/09/16'}, 'invalid_bulk_publication_clock'),
    ({'published':'2019-09-16T16:30:42'}, 'invalid_bulk_publication_clock'),
    ({'published':'2019-09-16T16:30+08:00'}, 'invalid_bulk_publication_clock'),
    ({'published':'2019-02-30T16:30:42+08:00'}, 'invalid_bulk_publication_clock'),
    ({'modified':'2019-09-16T16:20:00+08:00'}, 'invalid_bulk_publication_clock'),
    ({'day':'2019/09/15'}, 'invalid_bulk_publication_clock'),
    ({'day':'2019/02/30'}, 'invalid_bulk_publication_clock'),
])
@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
def test_unverified_identity_attribution_or_clock_is_not_replaced_by_related_footer_or_schema(changes, code, backend):
    source = replace(PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud'], html_parser=backend)
    with pytest.raises(NewsError, match=code):
        extract_article(page(**changes), source, page_url=URL)


@pytest.mark.parametrize('old,new,code', [
    ('<p>本篇新聞</p></h1>', '<p>其他標題</p></h1>', 'article_identity_not_verified'),
    ('<p>本篇新聞</p></h1>', '</h1><aside>本篇新聞</aside>', 'article_identity_not_verified'),
    ('<p>本篇新聞</p></h1>', '</h1><p>本篇新聞</p><p>不相關內容</p>', 'article_identity_not_verified'),
    ('href="/node/4524">版權宣告', 'href="/node/123">版權宣告', 'article_license_not_verified'),
    ('責任主編:', '記者:', 'article_attribution_not_verified'),
    ('property="article:published_time"', 'property="site:published_time"', 'invalid_bulk_publication_clock'),
    (f'property="og:updated_time" content="{MODIFIED}"', f'property="og:updated_time" content="{PUBLISHED}"', 'invalid_bulk_publication_clock'),
    ('class="group-author"', 'class="group-author" hidden', 'article_identity_not_verified'),
    ('<p>'+TEXT, '<p style="display:none">'+TEXT, 'article_identity_not_verified'),
])
@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
def test_source_shape_loss_hidden_prose_and_copyright_conflicts_fail_closed(old, new, code, backend):
    source = replace(PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud'], html_parser=backend)
    with pytest.raises(NewsError, match=code):
        extract_article(page().replace(old.encode(), new.encode()), source, page_url=URL)


@pytest.mark.parametrize('url', ['http://www.coolloud.org.tw/node/93414', 'https://coolloud.org.tw/node/93414',
    URL+'/', URL+'?view=1', URL+'#part', 'https://other.test/node/93414'])
def test_scope_does_not_create_an_alternate_route_host_or_identity(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page(url=url), PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud'], page_url=url)


@pytest.mark.parametrize('field,value', [
    ('id','other'), ('domains',['other.test']), ('require_author_attribution',False),
    ('require_configured_container',False), ('article_selectors',['article']),
    ('acquisition_mode','continuous'), ('embedded_article_layout','coolloud_unreviewed'),
    ('authorization',{'kind':'open_license','license':'CC-BY-4.0','evidence_url':COPYRIGHT}),
])
def test_original_license_and_native_recipe_cannot_transfer_to_unreviewed_scope(field, value):
    cfg = configured()
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('extra,code', [
    ('<div class="paywall">會員內容</div>', 'paywall_or_login'),
    ('<form action="/login"><input type="password"></form>', 'paywall_or_login'),
])
def test_actual_access_gate_remains_a_refusal(extra, code):
    with pytest.raises(NewsError, match=code):
        extract_article(page(extra=extra), PolicyRegistry(configured(), base=Path.cwd()).sources['coolloud'], page_url=URL)


def test_catalog_review_uses_one_canonical_finite_batch_and_binding_source_crawl_delay():
    cfg, registry, _ = load_config(Path(os.environ.get('STOCKAGENT_NEWS_SCOPE_CONFIG','configs/news_fulltext.json')))
    source = registry.sources['coolloud']
    assert source.allowed and source.acquisition_mode == ('continuous' if source.known_url_only else 'finite')
    if source.known_url_only:
        entry = next(p for p in cfg['providers'] if p['id'] == 'coolloud')
        assert entry['private_known_url_scope'] == 'publisher_original_open_license_v1'
    assert source.interval_seconds == 10 and source.parallelism == 1
    assert source.authorization['license'] == 'CC-BY-NC-SA-2.0-TW'
    assert not any((source.feeds,source.sitemaps,source.wordpress_apis,source.index_pages,source.metadata_feeds))
    assert cfg['publish'] is False and cfg['remote_training_export'] is False


def test_durable_chain_keeps_native_source_receipt_and_terminal_external_scope_no_restart_rescan(tmp_path):
    cfg = configured()
    external = 'https://www.coolloud.org.tw/node/36836'
    robots = 'https://www.coolloud.org.tw/robots.txt'
    http, session = http_fixture(cfg,tmp_path,{robots:FakeResponse(b'User-agent: *\nDisallow:\n'),
        URL:FakeResponse(page()),external:FakeResponse(page(url=external,node='36836',kind='news',source='工商時報'))})
    queue = NewsQueue(tmp_path,http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url':URL},{'url':external}])
        queue.sync_finite_cohorts()
        run_cycle(queue,http,cfg,max_articles=2,max_discovery_requests=0)
        rows = {r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert rows[URL]['state'] == 'complete' and rows[external]['state'] == 'article_rights_or_scope_excluded'
        assert not rows[external]['body_sha256']
        head = rows[URL]
        header = dict(queue.db.execute('SELECT * FROM finite_cohorts').fetchone())
        cohort_sha = sha256_file(tmp_path/header['receipt_path'])
        digest = membership_digest(queue.db,'coolloud')
        records = pq.read_table(tmp_path/head['parquet_path']).to_pylist()
        record = next(r for r in records if r['requested_url'] == URL)
        assert record['published_at_raw'] == PUBLISHED and record['modified_at_raw'] == MODIFIED
        assert json.loads(record['authors_json']) == ['本篇記者'] and record['raw_response_body'] == page()
        assert verify_bodies(tmp_path)['accepted']
        proof = audit(tmp_path,[URL],registry=http.registry)
        assert proof['accepted'],proof['errors']
        requests = len(session.calls)
        for _ in range(2):
            queue.reconcile_policy()
            queue.sync_finite_cohorts()
            run_cycle(queue,http,cfg,max_articles=2,max_discovery_requests=0)
        assert len(session.calls) == requests
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone()) == head
        assert membership_digest(queue.db,'coolloud') == digest and sha256_file(tmp_path/header['receipt_path']) == cohort_sha
        assert all('external.test' not in url and not url.endswith('/node/4524') for url,_ in session.calls)
    finally:
        http.close()
        queue.close()
