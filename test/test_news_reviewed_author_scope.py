"""Publisher-original body scope, not a blanket right inferred from a byline."""
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


@pytest.fixture
def source():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    return registry.sources['moneyweekly']


def page(author='理周投研部', *, schema_author=None, current_clock=True, article_id='254394'):
    text = '原始營收 -12.5、EPS 6.58，這是來源正文。' * 20
    schema = {'@type': 'Article', 'headline': '本篇財經報導', 'datePublished': '2026/10/8'}
    if schema_author:
        schema['author'] = {'name': schema_author}
    clock = '2026/10/8' if current_clock else ''
    if not current_clock:
        schema.pop('datePublished')
    return (f'<html><head><meta charset="utf-8"><link rel="canonical" '
            f'href="https://www.moneyweekly.com.tw/ArticleData/Info/Article/{article_id}"><script type="application/ld+json">'
            f'{json.dumps(schema, ensure_ascii=False)}</script></head><body><h1>本篇財經報導</h1>'
            f'<div><div>{clock}</div><div>{author}</div><div>2,000 瀏覽數</div></div>'
            '<div itemprop="articleBody"></div><div class="div_Article_Info" itemprop="articleBody">'
            f'<p>{text}</p><div data-widget_id="ad">不屬於正文的推薦廣告</div></div>'
            '<h2>延伸閱讀</h2><div>第三方作者 2099/1/1</div></body></html>').encode(), text


def test_verified_moneyweekly_exact_original_body_byline_and_date_only(source):
    payload, text = page()
    article = extract_article(payload, source, page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert article['body'] == text
    assert json.loads(article['authors_json']) == ['理周投研部']
    assert article['published_at_raw'] == '2026/10/8'
    assert article['extraction_method'] == 'css:.div_Article_Info[itemprop=articleBody]'
    assert source.allowed and source.acquisition_mode == 'finite'
    assert source.authorization['kind'] == 'personal_research'
    assert not source.feeds and not source.sitemaps and not source.index_pages


@pytest.mark.parametrize('author', ['第三方新聞社', '', '理周投研部 第三方作者', '相關報導作者'])
def test_unknown_or_third_party_current_byline_is_not_replaced_by_matching_schema_author(source, author):
    payload, _ = page(author, schema_author='理周投研部')
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(payload, source, page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_reviewed_current_author_wins_over_different_jsonld_author(source):
    payload, _ = page('洪崇晏', schema_author='其他作者', article_id='254340')
    article = extract_article(payload, source, page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254340')
    assert json.loads(article['authors_json']) == ['洪崇晏']


def test_missing_publication_clock_does_not_borrow_related_date(source):
    payload, _ = page(current_clock=False)
    article = extract_article(payload, source, page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert article['published_at_raw'] is None


def test_unconfigured_author_filter_retains_existing_source_semantics(source):
    payload, _ = page('其他作者')
    article = extract_article(payload, replace(source, permitted_author_names=()),
                              page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert json.loads(article['authors_json']) == ['其他作者']


@pytest.mark.parametrize('value', [True, '作者', [''], [' 作者'], ['作者', '作者'], [['作者']]])
def test_author_scope_rejects_unbounded_or_ambiguous_configuration(value):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = next(p for p in config['providers'] if p['id'] == 'moneyweekly')
    entry['permitted_author_names'] = value
    with pytest.raises(ValueError, match='invalid reviewed author scope'):
        PolicyRegistry(config, base=repo)


@pytest.mark.parametrize('key', ['author_selectors', 'require_author_attribution'])
def test_author_scope_requires_verified_current_article_byline(key):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = next(p for p in config['providers'] if p['id'] == 'moneyweekly')
    entry[key] = [] if key == 'author_selectors' else False
    with pytest.raises(ValueError, match='reviewed author scope requires'):
        PolicyRegistry(config, base=repo)


def test_author_scope_changes_data_fingerprint(source):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = next(p for p in config['providers'] if p['id'] == 'moneyweekly')
    entry['permitted_author_names'].append('新作者')
    assert PolicyRegistry(config, base=repo).sources['moneyweekly'].data_fingerprint != source.data_fingerprint


@pytest.mark.parametrize('value', [True, 'https://www.moneyweekly.com.tw/', [],
                                   ['http://www.moneyweekly.com.tw/'], ['https://other.test/'], [12]])
def test_author_scope_needs_bounded_original_publisher_evidence(value):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = next(p for p in config['providers'] if p['id'] == 'moneyweekly')
    entry['authorization']['original_author_scope_evidence_urls'] = value
    with pytest.raises(ValueError, match='reviewed author scope requires'):
        PolicyRegistry(config, base=repo)


def test_explicit_source_authorization_barriers_remain_disabled():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    assert not registry.sources['uanalyze'].allowed
    assert not registry.sources['wealth'].allowed
    assert not registry.sources['inside'].allowed


@pytest.mark.parametrize('path', [
    '/ArticleData/Info/Article/254394', '/ArticleData/Info/article/254394/',
    '/ArticleData/Info/理財周刊/254394', '/ArticleData/Info/%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A/254394',
    '/Home/ChangePage/Article/254394/', '/Home/ChangePage/%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A/254394/',
])
def test_source_legacy_routes_keep_original_identity_and_navigation_query(source, path):
    import re
    payload, text = page()
    assert re.fullmatch(source.article_path_pattern, path)
    article = extract_article(payload, source, page_url='https://www.moneyweekly.com.tw' + path + '?Type=3&Page=OriginalNavigation')
    assert article['body'] == text
    assert article['declared_canonical_url'] == 'https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394'
    assert article['published_at_raw'] == '2026/10/8'


@pytest.mark.parametrize('canonical', [
    'https://www.moneyweekly.com.tw/ArticleData/Info/Article/999',
    'https://elsewhere.test/ArticleData/Info/Article/254394',
    'https://www.moneyweekly.com.tw/Home/index',
    'https://www.moneyweekly.com.tw/Magazine/List/Article/254394',
    'https://www.moneyweekly.com.tw/ArticleData%2FInfo/Article/254394',
])
def test_legacy_route_cannot_take_related_foreign_or_unverified_canonical_body(source, canonical):
    payload, _ = page()
    payload = payload.replace(b'https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394', canonical.encode())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, source, page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_missing_canonical_is_an_identity_failure_not_an_empty_success(source):
    payload, _ = page()
    payload = payload.replace(b'rel="canonical"', b'rel="alternate"')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, source, page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_source_empty_template_is_distinct_from_missing_parser_or_article_text(source):
    url = 'https://www.moneyweekly.com.tw/ArticleData/Info/Article/212663'
    payload = (f'<html><head><link rel="canonical" href="{url}"></head><body>'
               '<h1></h1><div><div></div><div></div></div>'
               '<div class="div_Article_Info" itemprop="articleBody"></div></body></html>').encode()
    with pytest.raises(NewsError, match='source_article_empty_template'):
        extract_article(payload, source, page_url=url)


@pytest.mark.parametrize('preview', ['', '<p>' + '公開預覽但不是全文。' * 30 + '</p>'])
@pytest.mark.parametrize('login_link', ['', '<a href="/ArticleData/LoginNormalArticle/理財周刊/254394?From">登入會員</a>'])
def test_current_moneyweekly_member_mask_is_not_missing_body_or_complete_preview(source, preview, login_link):
    payload, _ = page()
    soup_text = payload.decode().replace(
        '<div class="div_Article_Info" itemprop="articleBody">',
        '<div class="div_Article_Info" itemprop="articleBody">' + preview +
        '<div class="mt-5">》本文限會員登入瀏覽，請先登入 ' + login_link + '</div>')
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(soup_text.encode(), source,
                        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_member_mask_cannot_be_replaced_by_long_jsonld_body(source):
    payload, text = page()
    payload = payload.decode().replace('"headline": "本篇財經報導",',
        '"headline": "本篇財經報導", "articleBody": ' + json.dumps(text, ensure_ascii=False) + ',')
    payload = payload.replace('<div class="div_Article_Info" itemprop="articleBody">',
        '<div class="div_Article_Info" itemprop="articleBody">'
        '<div class="mt-5">》本文限會員登入瀏覽，請先登入 登入會員</div>')
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(payload.encode(), source,
                        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_member_navigation_and_quoted_sentence_do_not_block_public_body(source):
    payload, text = page()
    payload = payload.decode().replace('<h1>',
        '<nav><div class="mt-5">》本文限會員登入瀏覽，請先登入 登入會員</div></nav><h1>', 1)
    payload = payload.replace(text, text + ' 文章引用「》本文限會員登入瀏覽，請先登入」這個網站說明。')
    article = extract_article(payload.encode(), source,
                              page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert article['body'].startswith(text)
    assert '文章引用' in article['body']


def test_retained_old_member_mask_verifies_refusal_not_body_acceptance(tmp_path):
    import hashlib
    from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
    from scripts.verify_news_source_diagnostics import verify
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    url = 'https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394'
    raw, _ = page()
    raw = raw.replace(b'<div class="div_Article_Info" itemprop="articleBody">',
        ('<div class="div_Article_Info" itemprop="articleBody">'
         '<div class="mt-5">》本文限會員登入瀏覽，請先登入 登入會員</div>').encode())
    digest = hashlib.sha256(raw).hexdigest()
    path = tmp_path / 'old-private-diagnostic.parquet'
    atomic_write_parquet(path, table_from_records([{
        'provider': 'moneyweekly', 'request_url': url, 'final_url': url,
        'response_sha256': digest, 'raw_response_body': raw}]))
    atomic_write_json(path.with_suffix('.receipt.json'), {
        'sha256': sha256_file(path), 'diagnostic_only': True,
        'accepted_as_article': False, 'publish': False})
    result = verify([{'provider': 'moneyweekly', 'url': url, 'status': 200,
                      'response_sha256': digest, 'private_diagnostic_path': str(path)}], registry)
    assert result['accepted'] and not result['all_sites_downloaded'] and not result['publish']
    assert result['results'][0]['source_outcome_verified']
    assert result['results'][0]['source_outcome'] == 'paywall_or_login'
    assert not result['results'][0]['parser_verified']


def test_non_body_queue_acceptance_is_exact_and_readonly(tmp_path):
    from scripts.verify_news_source_diagnostics import verify_queue_outcomes
    from downloader.news_fulltext_queue import NewsQueue
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    url = 'https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394'
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE url=?", (url,))
        result = [{'provider': 'moneyweekly', 'url': url, 'source_outcome_verified': True,
                   'source_outcome': 'paywall_or_login'}]
        verification = verify_queue_outcomes(result, tmp_path / 'queue.sqlite3', registry)
        assert verification['accepted'] and verification['read_only']
        assert not verification['rows'][0]['has_previous_stored_body']
        result[0]['url'] += '/unknown'
        assert not verify_queue_outcomes(result, tmp_path / 'queue.sqlite3', registry)['accepted']
        result[0]['url'] = url
        result[0]['source_outcome'] = 'source_not_found'
        assert not verify_queue_outcomes(result, tmp_path / 'queue.sqlite3', registry)['accepted']
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'paywall_or_login'
    finally:
        queue.close()


def test_exact_source_absence_redirect_does_not_become_an_article(source):
    from types import SimpleNamespace
    from downloader.news_fulltext import fetch_article
    from downloader.news_fulltext_http import Response
    response = Response('https://www.moneyweekly.com.tw/Error/Error404', 200, {'content-type': 'text/html'}, b'<p>Not an article</p>')
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda url: source), get=lambda url, headers: response)
    with pytest.raises(NewsError, match='source_not_found'):
        fetch_article(http, {'url': 'https://www.moneyweekly.com.tw/Home/ChangePage/Article/195112/?Type=3&Page=Original'})
