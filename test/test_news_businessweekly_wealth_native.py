"""Synthetic-only wealth writer/day/query/body contract and durable replay."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_businessweekly import (
    WEALTH_AUTHOR_SELECTOR, WEALTH_BODY_SELECTOR, WEALTH_EXCLUSIONS,
    WEALTH_LAYOUT, WEALTH_ORIGIN, WEALTH_PATH_PATTERN,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = WEALTH_ORIGIN+'/GArticle.aspx?id=ARTL000012345'
OUTSIDE = URL.replace('000012345', '000099999')
WRITER = '合成作者'
DAY = '2026.10.21'
TEXT = '自行撰寫合成資料：保留股數1000、價格12.34、負數-5與比率2.5%，沒有複製新聞。' * 5


def entry():
    return {'id': 'businessweekly_wealth', 'domains': ['wealth.businessweekly.com.tw'], 'enabled': True,
        'acquisition_mode': 'finite', 'finite_article_urls': [URL], 'finite_identity_query_keys': ['id'],
        'html_parser': 'lxml', 'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': WEALTH_LAYOUT, 'article_selectors': [WEALTH_BODY_SELECTOR],
        'article_path_pattern': WEALTH_PATH_PATTERN, 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'author_selectors': [WEALTH_AUTHOR_SELECTOR], 'permitted_author_names': [WRITER],
        'exclude_selectors': list(WEALTH_EXCLUSIONS),
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-22',
            'evidence_url': 'https://www.businessweekly.com.tw/CopyRight',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'original_author_scope_evidence_urls': [URL],
            'scope': 'One synthetic finite private reading; no sitewide, paid, training or public grant.'}}


def settings(candidate=None):
    return {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research', 'providers': [candidate or entry()]}


def policy(candidate=None):
    return PolicyRegistry(settings(candidate), base=Path('.')).sources['businessweekly_wealth']


def page():
    picture = 'https://iwealth.bwnet.com.tw/AC_Gallery/2026/10/synthetic.jpg'
    return BeautifulSoup(f'''<html><head><link rel="canonical" href="{URL}">
      <meta property="og:url" content="http:{URL[6:]}">
      <meta property="og:title" content="合成標題"></head><body><nav>NOT_ARTICLE_NAV</nav>
      <input type="hidden" id="wealth_ArticleURL" value="{URL}">
      <input type="hidden" id="wealth_ArticleWriter" value="{WRITER}">
      <input type="hidden" id="wealth_ArticlePostDate" value="{DAY}">
      <div id="main_cnt"><div class="titxshr"><h1>合成標題</h1></div>
      <div class="articlefont smart"><div class="article_function2" style="display:none">字級按鈕</div>
      <div class="article_date"><a class="write"><span>撰文者：{WRITER}</span></a>
      <span>{DAY}</span><span class="counts">瀏覽數：1000</span></div></div>
      <div class="article_main"><div class="summary-wrap smart"><p>合成摘要保留。</p></div>
      <adcover></adcover><p>{TEXT}<br>下一行也保留。</p>
      <ins pid="E1F2C955-1503-43EE-96C2-C665D8F11658"></ins><h2>合成小標</h2>
      <p>合成結論。</p><div class="article_kv"><a href="{picture}"><img src="{picture}" alt=""></a>
      <div class="imgtxt_wrp"><div class="imgtxt">&nbsp;</div><a href="{picture}"><div class="img-zoom">圖片放大</div></a></div></div>
      <!-- NOT_ARTICLE_COMMENT --></div>
      <div class="pagearea"><ul><li class="first"><span title="第一頁">第一頁</span></li>
      <li class="prepage"><span>«</span></li><li><span class="select">1</span></li>
      <li class="nextpage"><span>»</span></li><li class="last"><span title="最後一頁">最後頁</span></li>
      <li class="goback"><a href="index.aspx">首頁</a></li><li class="single"><a href="{URL}&amp;p=0">單頁閱讀</a></li></ul></div>
      <div class="authorinfo smart">NOT_ARTICLE_BRAND<!-- authorname 不可拿來當作者 --></div></div>
      <aside>NOT_ARTICLE_RELATED</aside><footer>NOT_ARTICLE_FOOTER</footer></body></html>''', 'lxml')


def extract(soup, *, url=URL):
    return extract_article(str(soup).encode(), policy(), page_url=url)


def test_written_values_summary_writer_and_publication_day_are_retained_without_fabricated_time():
    article = extract(page())
    assert all(value in article['body'] for value in (TEXT, '合成摘要保留。', '合成小標', '合成結論。', '下一行也保留。'))
    assert '圖片放大' not in article['body'] and 'NOT_ARTICLE_' not in article['body']
    assert json.loads(article['authors_json']) == [WRITER]
    assert article['body_sha256'] == hashlib.sha256(article['body'].encode()).hexdigest()
    assert article['published_at_raw'] == DAY and article['modified_at_raw'] is None
    assert not article['historical_point_in_time'] and not article['per_article_license_verified']
    attribution = json.loads(article['author_attribution_detail_json'])
    assert attribution['source_visible_credit'] == '撰文者：'+WRITER
    assert not attribution['original_ownership_verified']
    notices = json.loads(article['verified_article_notices_json'])
    assert notices['clock_precision'] == 'day' and not notices['intraday_publication_time_known']
    assert notices['single_visible_page_verified'] and notices['OG_HTTP_declaration_not_requested_or_used_as_alias']
    assert notices['validated_empty_ad_shells_excluded'] == 2
    assert notices['validated_captionless_media_shells_excluded'] == 1
    assert not notices['media_or_scripts_requested_or_executed'] and not notices['old_metadata_used_as_publication']


@pytest.mark.parametrize('url', [OUTSIDE, URL+'&p=0', URL+'&extra=1', URL.replace('id=', 'ID='),
    URL.replace('ARTL', 'artl'), URL.replace('https:', 'http:'), URL.replace('wealth.', 'smart.'),
    URL+'#fragment', URL.replace('.tw/', '.tw:443/')])
def test_literal_article_ID_host_and_query_are_not_alias_inference(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(), url=url)


@pytest.mark.parametrize('failure,error', [
    ('canonical', 'article_identity_not_verified'), ('hidden_url', 'article_identity_not_verified'),
    ('OG_URL', 'article_identity_not_verified'), ('title', 'article_identity_not_verified'),
    ('duplicate_title', 'article_identity_not_verified'), ('duplicate_writer', 'article_identity_not_verified'),
    ('writer', 'article_attribution_not_verified'), ('missing_writer', 'article_identity_not_verified'),
    ('prefix', 'article_attribution_not_verified'), ('hidden_writer', 'article_identity_not_verified'),
    ('linked_writer', 'article_attribution_not_verified'), ('byline_injected_child', 'article_attribution_not_verified'),
    ('clock', 'invalid_bulk_publication_clock'), ('invalid_day', 'invalid_bulk_publication_clock'),
    ('timestamp', 'invalid_bulk_publication_clock'), ('extra_clock', 'invalid_bulk_publication_clock'),
    ('hidden_clock', 'article_identity_not_verified'), ('second_page', 'article_body_incomplete'),
    ('next_page', 'article_identity_not_verified'), ('changed_all_link', 'article_body_incomplete'),
    ('hidden_body', 'article_identity_not_verified'), ('hidden_prose', 'article_identity_not_verified'),
    ('script', 'article_identity_not_verified'), ('iframe', 'article_identity_not_verified'),
    ('prose_in_ad', 'article_identity_not_verified'), ('new_ad_attr', 'article_identity_not_verified'),
    ('image_caption', 'article_identity_not_verified'), ('image_alt', 'article_identity_not_verified'),
    ('media_prose', 'article_identity_not_verified'), ('extra_media_child', 'article_identity_not_verified'),
    ('hidden_media', 'article_identity_not_verified'), ('third_party', 'article_rights_or_scope_excluded'),
    ('body_absent', 'article_identity_not_verified'), ('schema_body', 'body_not_found'),
])
def test_fail_closed_for_unreviewed_identity_credit_clock_page_or_body(failure, error):
    soup = page()
    body = soup.select_one('.article_main')
    if failure == 'canonical': soup.select_one('link[rel=canonical]')['href'] = OUTSIDE
    elif failure == 'hidden_url': soup.select_one('#wealth_ArticleURL')['value'] = OUTSIDE
    elif failure == 'OG_URL': soup.select_one('meta[property="og:url"]')['content'] = OUTSIDE
    elif failure == 'title': soup.h1.string = '其他標題'
    elif failure == 'duplicate_title': soup.select_one('.titxshr').append(deepcopy(soup.h1))
    elif failure == 'duplicate_writer': soup.body.append(deepcopy(soup.select_one('#wealth_ArticleWriter')))
    elif failure == 'writer': soup.select_one('#wealth_ArticleWriter')['value'] = '其他人'
    elif failure == 'missing_writer': soup.select_one('#wealth_ArticleWriter').decompose()
    elif failure == 'prefix': soup.select_one('.write span').string = '轉載：'+WRITER
    elif failure == 'hidden_writer': soup.select_one('.write span')['hidden'] = ''
    elif failure == 'linked_writer': soup.select_one('.write')['href'] = '/third-party'
    elif failure == 'byline_injected_child': soup.select_one('.write span').append(soup.new_tag('span'))
    elif failure == 'clock': soup.select_one('#wealth_ArticlePostDate')['value'] = '2026.10.20'
    elif failure in {'invalid_day', 'timestamp'}:
        value = '2026.02.30' if failure == 'invalid_day' else '2026.10.21 00:00:00'
        soup.select_one('#wealth_ArticlePostDate')['value'] = value
        soup.select_one('.article_date > span').string = value
    elif failure == 'extra_clock': soup.select_one('.article_date').append(soup.new_tag('span'))
    elif failure == 'hidden_clock': soup.select_one('.article_date > span')['style'] = 'display: none'
    elif failure == 'second_page': soup.select_one('.pagearea ul').append(BeautifulSoup('<li><a>2</a></li>', 'lxml').li)
    elif failure == 'next_page': soup.select_one('.nextpage span').name = 'a'
    elif failure == 'changed_all_link': soup.select_one('.single a')['href'] = OUTSIDE+'&p=0'
    elif failure == 'hidden_body': body['aria-hidden'] = 'true'
    elif failure == 'hidden_prose': body.p['style'] = 'visibility: hidden'
    elif failure in {'script', 'iframe'}: body.append(soup.new_tag(failure))
    elif failure == 'prose_in_ad': body.ins.string = TEXT
    elif failure == 'new_ad_attr': body.adcover['data-new-layout'] = 'unknown'
    elif failure == 'image_caption': body.select_one('.imgtxt').string = '圖中原始數字123'
    elif failure == 'image_alt': body.img['alt'] = '原始圖說123'
    elif failure == 'media_prose': body.select_one('.article_kv').append('原始正文不可以刪除')
    elif failure == 'extra_media_child': body.select_one('.article_kv').append(soup.new_tag('script'))
    elif failure == 'hidden_media': body.img['hidden'] = ''
    elif failure == 'third_party': body.p.string = '本文獲第三方授權轉載，原文。'+TEXT
    elif failure == 'body_absent': body.decompose()
    elif failure == 'schema_body':
        body.clear()
        node = soup.new_tag('script', type='application/ld+json')
        node.string = json.dumps({'@type': 'NewsArticle', 'articleBody': TEXT*10, 'author': {'name': WRITER}})
        soup.head.append(node)
    with pytest.raises(NewsError, match=error): extract(soup)


@pytest.mark.parametrize('field,value', [
    ('id', 'businessweekly'), ('domains', ['businessweekly.com.tw']), ('acquisition_mode', 'continuous'),
    ('finite_article_urls', []), ('finite_identity_query_keys', []), ('allow_reviewed_supplements', True),
    ('html_parser', 'html.parser'), ('article_selectors', ['body']), ('exclude_selectors', []),
    ('author_selectors', ['[rel=author]']), ('permitted_author_names', []),
    ('require_configured_container', False), ('require_author_attribution', False),
    ('feeds', [WEALTH_ORIGIN+'/feed']), ('sitemaps', [WEALTH_ORIGIN+'/sitemap.xml']),
])
def test_native_registration_is_closed_to_reviewed_finite_scope(field, value):
    candidate = entry(); candidate[field] = value
    with pytest.raises(ValueError): policy(candidate)


def test_native_verified_writer_still_requires_reviewed_name_permission():
    candidate = entry(); candidate['permitted_author_names'] = ['其他人']
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(str(page()).encode(), policy(candidate), page_url=URL)


def test_literal_NUL_is_not_silently_removed_by_HTML_parser():
    raw = str(page()).encode().replace(WRITER.encode(), '合成\x00作者'.encode())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(raw, policy(), page_url=URL)


def test_deployed_finite_wealth_identity_inherits_only_its_reviewed_diagnostic_controls():
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_businessweekly import WEALTH_FINITE_HTTP_BINDING

    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['businessweekly_wealth']
    assert source.domains == ('wealth.businessweekly.com.tw',)
    assert (source.data_fingerprint, source.authorization_fingerprint) == WEALTH_FINITE_HTTP_BINDING[:2]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == WEALTH_FINITE_HTTP_BINDING[2:]
    assert registry.match('https://smart.businessweekly.com.tw/Reading/IndepArticle.aspx?id=12345').provider == 'businessweekly'
    assert not registry.sources['businessweekly'].allowed
    synthetic = policy()
    assert not synthetic.http_capacity_source_fingerprint and not synthetic.http_capacity_authorization_fingerprint


@pytest.fixture
def finite_original(tmp_path):
    from test_news_retained_native_replay import approved
    config = settings()
    original_auth = deepcopy(entry()['authorization'])
    original_auth.pop('original_author_scope_evidence_urls')
    original_auth['scope'] = 'Original finite public diagnostic; not an accepted body.'
    result = approved(tmp_path, config, urls=(URL,), provider='businessweekly_wealth', original_auth=original_auth,
        raw_factory=lambda **kwargs: str(page()).encode())
    queue, _, path, manifest = result
    queue.add_articles([{'url': URL, 'date': '2017-01-02 09:01:00'}, {'url': OUTSIDE}])
    queue.sync_finite_cohorts()
    directory = path.parent/'private-source-diagnostics'
    directory.chmod(0o700)
    for p in directory.iterdir(): p.chmod(0o600)
    manifest.update(contract_version=3, scope_kind='reviewed_finite_native_materialization_v3',
        original_author_scope_evidence_addition_reviewed=True, diagnostic_directory=directory.name)
    atomic_write_json(path, manifest, durable=True)
    yield result
    queue.close()


def test_finite_replay_uses_existing_writer_clock_raw_receipts_once_and_no_HTTP(finite_original, monkeypatch):
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import head
    queue, _, path, manifest = finite_original
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('offline replay must not create a publisher HTTP owner'))
    before = head(queue, OUTSIDE)
    result = materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)
    assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
    current = head(queue, URL)
    row, = pq.ParquetFile(queue.root/current['parquet_path']).read(use_threads=False).to_pylist()
    assert row['published_at_raw'] == DAY and row['modified_at_raw'] is None
    assert row['metadata_date_raw'] == '2017-01-02 09:01:00' and not row['historical_point_in_time']
    assert row['source_observed_at_utc'] == row['captured_at_utc'] == manifest['entries'][0]['captured_at_utc']
    assert row['http_duration_seconds'] is None and row['body_sha256'] == hashlib.sha256(row['body'].encode()).hexdigest()
    assert sha256_file(queue.root/row['replay_source_parquet']) == manifest['entries'][0]['diagnostic_sha256']
    assert materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)['materialized_bodies'] == 0
    assert head(queue, URL) == current and head(queue, OUTSIDE) == before
    assert queue.claim_url(OUTSIDE, 300) is None


def test_child_host_split_is_specific_preserves_other_hosts_and_all_existing_capture_facts(tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_retained_native_replay import head
    parent = {'id': 'businessweekly', 'domains': ['businessweekly.com.tw'], 'enabled': False,
        'authorization': {'kind': 'unknown'}}
    old_config = settings(parent)
    root = tmp_path/'news'
    old = NewsQueue(root, PolicyRegistry(old_config, base=tmp_path))
    main = 'https://www.businessweekly.com.tw/business/blog/12345'
    smart = 'https://smart.businessweekly.com.tw/Reading/IndepArticle.aspx?id=12345'
    with old.db:
        old.add_articles([{'url': u, 'date': '2016-01-02', 'title': '原始中繼資料'} for u in (URL, OUTSIDE, main, smart)])
    original = {u: head(old, u) for u in (URL, OUTSIDE, main, smart)}
    old.close()
    config = settings(); config['providers'].append(parent)
    current = NewsQueue(root, PolicyRegistry(config, base=tmp_path))
    try:
        assert current.registry.match(URL).provider == 'businessweekly_wealth'
        assert current.registry.match(smart).provider == 'businessweekly'
        for u in (main, smart): assert head(current, u) == original[u]
        for u in (URL, OUTSIDE):
            row = head(current, u)
            assert row['provider'] == 'businessweekly_wealth'
            for key in ('id', 'url', 'title', 'metadata_date_raw', 'attempts', 'body_sha256', 'parquet_path', 'captured_at_utc', 'last_checked_at_utc'):
                assert row[key] == original[u][key]
        current.sync_finite_cohorts()
        assert head(current, URL)['state'] == 'pending'
        # Adding a child source must not activate the parent's other heads.
        assert head(current, OUTSIDE)['state'] == original[OUTSIDE]['state'] == 'awaiting_authorization'
        assert current.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 4
    finally:
        current.close()
