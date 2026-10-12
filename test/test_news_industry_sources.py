from pathlib import Path
import json

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_fulltext import config


@pytest.fixture
def registry():
    return load_config(Path('configs/news_fulltext.json'))[1]


def test_genet_source_scope_uses_article_h2_and_never_section_h1(registry):
    source = registry.sources['genetinfo']
    text = '這是保留原始數值 -12.5 與公司公告的正文。' * 20
    payload = f'''<html><head><meta charset="utf-8"></head><body>
    <h1>分類，不是文章標題</h1><div class="post-content-single"><div class="post-item">
    <div class="post-title"><h2>來源本篇標題</h2></div>
    <div class="post-info"><span class="post-date">日期 : 2020/3/2</span><span class="post-autor">作者 : Genet</span></div>
    <div class="post-content-details"><p>{text}</p></div><ul class="pager">上一篇不屬於正文</ul>
    </div></div><aside><div class="post-info"><span class="post-date">2099/1/1</span></div>側欄</aside>
    </body></html>'''.encode()
    article = extract_article(payload, source, page_url='https://www.genetinfo.com/investment/company-news/item/123.html')
    assert article['title'] == '來源本篇標題'
    assert article['body'] == text
    assert article['published_at_raw'] == '日期 : 2020/3/2'
    assert json.loads(article['authors_json']) == ['作者 : Genet']


def test_reviewed_short_source_announcement_is_not_rejected_by_arbitrary_length(registry):
    from dataclasses import replace
    source = registry.sources['genetinfo']
    text = '董事會決議庫藏股實施期間及買回數量、價格區間。'
    payload = f'''<div class="post-content-single"><div class="post-item">
    <div class="post-title"><h2>本篇公告標題</h2></div>
    <div class="post-info"><span class="post-date">日期 : 2023/10/13</span><span class="post-autor">作者 : 來源作者</span></div>
    <div class="post-content-details"><p>{text}</p></div></div></div>'''.encode()
    article = extract_article(payload, source, page_url='https://www.genetinfo.com/investment/featured/item/90038.html')
    assert article['body'] == text and article['published_at_raw'] == '日期 : 2023/10/13'
    assert json.loads(article['authors_json']) == ['作者 : 來源作者']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(payload, replace(source, allow_short_configured_body=False))
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(payload.replace('作者 : 來源作者'.encode(), b''), source)
    # Only the exact body node is admitted, never a headline or unrelated teaser.
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(payload.replace(b'post-content-details', b'unrelated'), source)


def test_short_source_policy_cannot_disable_container_identity_checks(config, tmp_path):
    from downloader.news_fulltext_policy import PolicyRegistry
    config['providers'][0]['allow_short_configured_body'] = True
    with pytest.raises(ValueError, match='short-body acceptance requires'):
        PolicyRegistry(config, base=tmp_path)


def pansci_page(*, source_id='318015', source_url='https://pansci.asia/archives/318015', date=True):
    text = '目標篇的原始負值 -12.5 與科學資料。' * 20
    clock = '<span class="post-text-light">・2021/04/12</span>' if date else ''
    return f'''<html><head><meta charset="utf-8"></head><body>
    <section id="load_post_1" data-type="post" data-post_id="{source_id}" data-url="{source_url}">
    <div class="post-title-box"><h1>本篇</h1></div><div class="row"><span class="post-text-blue"><a rel="author">本篇作者</a></span>{clock}</div>
    <div class="post-content-container"><div class="pansc-content">廣告文字</div><p>{text}</p></div></section>
    <section id="load_post_2" data-type="post" data-post_id="999"><h1>其他文章</h1>
    <div class="post-title-box"></div><div class="row"><span class="post-text-blue"><a rel="author">別篇作者</a></span>
    <span class="post-text-light">・2099/1/1</span></div><div class="post-content-container">別篇正文</div>
    <meta property="article:published_time" content="2099-01-01"></section></body></html>'''.encode(), text


def test_pansci_scopes_body_byline_and_clock_to_identity_verified_first_post(registry):
    payload, text = pansci_page()
    article = extract_article(payload, registry.sources['pansci'], page_url='https://pansci.asia/archives/318015')
    assert article['body'] == text
    assert article['title'] == '本篇'
    assert json.loads(article['authors_json']) == ['本篇作者']
    assert article['published_at_raw'] == '・2021/04/12'


@pytest.mark.parametrize('source_id,source_url', [
    ('999', 'https://pansci.asia/archives/318015'),
    ('318015', 'https://pansci.asia/archives/999'), ('318015', ''),
    ('318015', 'https://other.test/archives/318015'),
])
def test_pansci_wrong_article_identity_is_not_accepted(registry, source_id, source_url):
    payload, _ = pansci_page(source_id=source_id, source_url=source_url)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, registry.sources['pansci'], page_url='https://pansci.asia/archives/318015')


def test_pansci_missing_current_clock_never_borrows_related_article_clock(registry):
    payload, _ = pansci_page(date=False)
    article = extract_article(payload, registry.sources['pansci'], page_url='https://pansci.asia/archives/318015')
    assert article['published_at_raw'] is None


def test_pansci_supplement_opt_in_does_not_release_unreviewed_metadata_growth(registry, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue

    original_url = 'https://pansci.asia/archives/318015'
    unreviewed_url = 'https://pansci.asia/archives/333349'
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{'url': original_url}])
        queue.sync_finite_cohorts()
        original = dict(queue.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', ('pansci',)).fetchone())
        with queue.db:
            queue.add_articles([{'url': unreviewed_url}])
        queue.sync_finite_cohorts()
        assert registry.sources['pansci'].allow_reviewed_supplements
        assert queue.claim_url(unreviewed_url, 300, refresh=True) is None
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (unreviewed_url,)).fetchone()[0] == 'waiting_cohort'
        assert dict(queue.db.execute('SELECT * FROM finite_cohorts WHERE provider=?', ('pansci',)).fetchone()) == original
        assert queue.db.execute('SELECT count(*) FROM finite_supplement_members WHERE provider=?', ('pansci',)).fetchone()[0] == 0
    finally:
        queue.close()


def test_new_sources_reuse_finite_shared_collector_and_keep_refused_gbi_disabled(registry):
    for provider in ('genetinfo', 'pansci'):
        source = registry.sources[provider]
        assert source.allowed and source.acquisition_mode == 'finite'
        assert source.require_author_attribution and source.html_parser == 'lxml'
        assert not source.feeds and not source.sitemaps and not source.index_pages
        assert source.authorization['kind'] == 'personal_research'
    assert registry.sources['pansci'].allow_reviewed_supplements
    assert registry.sources['genetinfo'].allow_reviewed_supplements
    assert not registry.sources['gbimonthly'].allowed
    assert not registry.sources['inside'].allowed
    assert registry.sources['inside'].acquisition_mode == 'finite'
    assert registry.sources['inside'].authorization['automated_access_review'] == 'explicit_prohibition_identified'


def test_genet_source_only_image_is_classified_not_boilerplate_or_fake_text(registry):
    html = b'<div class="post-content-single"><div class="post-item"><div class="post-content-details"><p><img src="/report.png"></p></div></div></div>'
    with pytest.raises(NewsError, match='source_body_image_only'):
        extract_article(html, registry.sources['genetinfo'], page_url='https://www.genetinfo.com/investment/featured/item/93126.html')


def test_verified_genet_soft404_is_absent_not_a_successful_article(config, tmp_path):
    from downloader.news_fulltext import fetch_article
    from test_news_fulltext import FakeResponse, http_fixture
    config['providers'][0]['source_not_found_paths'] = ['/404.html']
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/a': FakeResponse(status=302, headers={'Location': '/404.html?errorGo=2'}),
        'https://publisher.test/404.html?errorGo=2': FakeResponse(b'<html>Page not found</html>'),
    })
    try:
        with pytest.raises(NewsError, match='source_not_found'):
            fetch_article(http, {'url': 'https://publisher.test/a'})
    finally:
        http.close()


def test_namespace_repair_requeues_only_supported_legacy_outcomes_not_absence_or_refusals(config, tmp_path):
    from downloader.news_fulltext_policy import PolicyRegistry
    from downloader.news_fulltext_queue import NewsQueue
    config['providers'][0].update(id='inside', article_path_pattern='^/article/[0-9]+$')
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/feature/topic/1'},
                                {'url': 'https://publisher.test/article/2'},
                                {'url': 'https://publisher.test/article/3'}])
            queue.db.execute("UPDATE articles SET state='unsupported_article_path' WHERE url LIKE '%/1'")
            queue.db.execute("UPDATE articles SET state='source_not_found' WHERE url LIKE '%/2'")
            queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',due=9999999999 WHERE url LIKE '%/3'")
        config['providers'][0]['article_path_pattern'] = '^/(article|feature/topic)/[0-9]+$'
        queue.registry = PolicyRegistry(config, base=tmp_path)
        queue.reconcile_policy()
        assert queue.db.execute("SELECT state FROM articles WHERE url LIKE '%/1'").fetchone()[0] == 'pending'
        assert queue.db.execute("SELECT state FROM articles WHERE url LIKE '%/2'").fetchone()[0] == 'source_not_found'
        assert queue.db.execute("SELECT state,due FROM articles WHERE url LIKE '%/3'").fetchone()[:] == ('retry', 9999999999)
    finally:
        queue.close()


def test_short_body_repair_requeues_parser_failure_without_replaying_source_refusal(config, tmp_path):
    from downloader.news_fulltext_policy import PolicyRegistry
    from downloader.news_fulltext_queue import NewsQueue
    config['providers'][0].update(require_configured_container=True, require_author_attribution=True,
                                title_selector='h1', publication_selectors=['time'], article_path_pattern='^/article/[0-9]+$')
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/article/{number}'} for number in range(4)])
            queue.db.execute("UPDATE articles SET state='body_not_found' WHERE url LIKE '%/0'")
            queue.db.execute("UPDATE articles SET state='source_body_image_only' WHERE url LIKE '%/1'")
            queue.db.execute("UPDATE articles SET state='source_not_found' WHERE url LIKE '%/2'")
            queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',due=9999999999 WHERE url LIKE '%/3'")
        config['providers'][0]['allow_short_configured_body'] = True
        queue.registry = PolicyRegistry(config, base=tmp_path)
        queue.reconcile_policy()
        assert queue.db.execute("SELECT state FROM articles WHERE url LIKE '%/0'").fetchone()[0] == 'pending'
        assert queue.db.execute("SELECT state FROM articles WHERE url LIKE '%/1'").fetchone()[0] == 'source_body_image_only'
        assert queue.db.execute("SELECT state FROM articles WHERE url LIKE '%/2'").fetchone()[0] == 'source_not_found'
        assert queue.db.execute("SELECT state,due FROM articles WHERE url LIKE '%/3'").fetchone()[:] == ('retry', 9999999999)
    finally:
        queue.close()
