"""Publisher-original body scope, not a blanket right inferred from a byline."""
from dataclasses import replace
from copy import deepcopy
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
    assert source.allowed and source.acquisition_mode == 'continuous'
    assert source.authorization['kind'] == 'personal_research'
    assert not source.feeds and not source.sitemaps and not source.index_pages


def reviewed_moneyweekly_config():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    entry = next(p for p in candidate['providers'] if p['id'] == 'moneyweekly')
    entry['permitted_author_names'] = ['理周投研部', '洪崇晏', '林庭頡']
    entry['authorization']['reviewed_on'] = '2026-10-24'
    entry['authorization']['original_author_scope_evidence_urls'] = [
        'https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394',
        'https://www.moneyweekly.com.tw/ArticleData/Info/Article/254340',
        'https://www.moneyweekly.com.tw/Home/ChangePage/Article/195112/',
        'https://www.moneyweekly.com.tw/Magazine/List/%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A/12109',
        'https://www.moneyweekly.com.tw/ArticleData/Info/Article/147720']
    return candidate


def test_short_parser_and_reviewed_contributor_preserve_exact_old_http_refusal_identity():
    config = reviewed_moneyweekly_config()
    source = PolicyRegistry(config, base=Path.cwd()).sources['moneyweekly']
    assert source.http_capacity_source_fingerprint == '66a7bdb4cd1750deeb470285f7c9d6df67f4b7d663a3e7ace0e6a0935fb56d3e'
    assert source.http_capacity_authorization_fingerprint == '275aa8eb6ea29a31f0668b6348bc7213e89bbd54c5b702adc1a95566ca265f3c'
    assert source.data_fingerprint != source.http_capacity_source_fingerprint
    assert source.authorization_fingerprint != source.http_capacity_authorization_fingerprint


@pytest.mark.parametrize('change', ['interval', 'parallel', 'domain', 'scope', 'author'])
def test_short_parser_http_compatibility_cannot_hide_other_transport_or_authorization_changes(change):
    config = reviewed_moneyweekly_config()
    entry = next(p for p in config['providers'] if p['id'] == 'moneyweekly')
    if change == 'interval': entry['interval_seconds'] = 9.25
    elif change == 'parallel': entry['capacity_max_parallelism'] = 3
    elif change == 'domain': entry['domains'] = ['www.moneyweekly.com.tw']
    elif change == 'scope': entry['authorization']['scope'] += ' Additional unreviewed grant.'
    else: entry['permitted_author_names'].append('未審查作者')
    source = PolicyRegistry(config, base=Path.cwd()).sources['moneyweekly']
    assert not source.http_capacity_source_fingerprint
    assert not source.http_capacity_authorization_fingerprint


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


def test_moneyweekly_structure_keeps_unknown_byline_as_metadata_without_exporting_prose(source):
    from bs4 import BeautifulSoup
    from scripts.probe_news_sources import moneyweekly_public_structure
    payload, prose = page('尚未核對的作者')
    soup = BeautifulSoup(payload, 'lxml')
    before = str(soup)
    result = moneyweekly_public_structure(soup, source,
        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert result['current_header_authors'] == [
        {'label': '尚未核對的作者', 'reviewed_name': False, 'links': []}]
    assert result['current_header_clocks'] == ['2026/10/8']
    assert result['body_nodes'][0]['paragraphs'] == 1
    assert result['body_nodes'][0]['normalized_characters'] > 100
    assert prose not in json.dumps(result, ensure_ascii=False)
    assert result['article_accepted'] is result['raw_retained'] is False
    assert result['source_permission_changed'] is False
    assert str(soup) == before


def test_moneyweekly_structure_does_not_borrow_related_author_or_publication_date(source):
    from bs4 import BeautifulSoup
    from scripts.probe_news_sources import moneyweekly_public_structure
    payload, _ = page('', current_clock=False)
    result = moneyweekly_public_structure(BeautifulSoup(payload, 'lxml'), source,
        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert result['current_header_authors'][0]['label'] == ''
    assert result['current_header_clocks'] == ['']
    assert '第三方作者' not in json.dumps(result, ensure_ascii=False)
    assert '2099/1/1' not in json.dumps(result, ensure_ascii=False)


def test_moneyweekly_structure_preserves_native_access_mask_and_identity_failure(source):
    from bs4 import BeautifulSoup
    from scripts.probe_news_sources import moneyweekly_public_structure
    payload, prose = page()
    payload = payload.replace(('<p>' + prose + '</p>').encode(),
        ' <div class="mt-5">》本文限會員登入瀏覽，請先登入</div>'.encode())
    result = moneyweekly_public_structure(BeautifulSoup(payload, 'lxml'), source,
        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert result['article_identity_or_access_error'] == 'paywall_or_login'
    result = moneyweekly_public_structure(BeautifulSoup(payload, 'lxml'), source,
        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/999')
    assert result['article_identity_or_access_error'] == 'article_identity_not_verified'


SHORT_PROSE = '公司(1234)公告6月營收 -12.50 億，月增2.23%、年增12.75%；累計營收2607.29億，年增34.54%。'


def short_page(*, prose=SHORT_PROSE, author='理周投研部', current_clock=True):
    payload, long_prose = page(author, current_clock=current_clock)
    editor = (f'<p>{prose}</p><p><img src="/Photo/Article/example.png" alt="" title="" style="width: 60%;"></p>'
        '<div class="cols-12 d-flex"><div class="col-11 py-3 div_Article_Info">圖片出處:理周圖庫</div></div>')
    return payload.replace(('<p>' + long_prose + '</p>').encode(), editor.encode())


def test_complete_native_moneyweekly_short_prose_preserves_raw_numbers_and_excludes_media_slots(source):
    payload = short_page()
    article = extract_article(payload, source,
        page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    assert article['body'] == SHORT_PROSE
    assert article['extraction_method'] == 'moneyweekly_visible_short_prose_v1'
    assert article['published_at_raw'] == '2026/10/8'
    assert json.loads(article['authors_json']) == ['理周投研部']
    assert article['body_completeness'].startswith('verified_publisher_short_body;')
    assert '圖片出處' not in article['body']
    assert '推薦廣告' not in article['body']


@pytest.mark.parametrize('prose', ['', '圖片出處 : 理周圖庫', '閱讀全文請先登入會員',
    '圖／來源單位', '<a href="https://other.test/">' + SHORT_PROSE + '</a>'])
def test_short_caption_access_notice_or_link_is_never_promoted_to_complete_prose(source, prose):
    with pytest.raises(NewsError, match='body_not_found|paywall_or_login'):
        extract_article(short_page(prose=prose), replace(source, allow_short_configured_body=True),
            page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_short_prose_does_not_hide_extra_unclassified_financial_block(source):
    payload = short_page().replace(b'</p><p><img', '</p><div>其他數值 -999</div><p><img'.encode(), 1)
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(payload, source,
            page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_short_prose_rejects_hidden_paragraph_and_duplicate_native_editor(source):
    payload = short_page().replace(('<p>' + SHORT_PROSE).encode(),
        ('<p style="display:none">' + SHORT_PROSE).encode())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, source,
            page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    payload = short_page() + b'<div class="div_Article_Info" itemprop="articleBody"><p>duplicate</p></div>'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, source,
            page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


def test_short_prose_needs_reviewed_current_author_and_current_source_date(source):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(short_page(author='未知作者'), source,
            page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(short_page(current_clock=False), source,
            page_url='https://www.moneyweekly.com.tw/ArticleData/Info/Article/254394')


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
