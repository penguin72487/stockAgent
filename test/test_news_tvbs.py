"""TVBS three native layouts, literal requests, source clocks and private scope."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_tvbs import TVBS_BODY_SELECTOR, TVBS_HOSTS, TVBS_PATH_PATTERN, tvbs_request_identity
from test_news_fulltext import FakeResponse, http_fixture


URLS = ('https://news.tvbs.com.tw/money/4035845', 'https://cars.tvbs.com.tw/car-news/342246',
    'https://supertaste.tvbs.com.tw/food/360398')
TITLE = '原始報導：-12.5%、123456股與比率0.75'
PROSE = '報導原始數值-12.5%、123456股與0.75不變；正文不是摘要、推薦新聞或圖片。' * 5
NOTICE = '研究原文風險提示：讀者應自行查證資料，不能把原始報導當保證收益。'


def recipe(parser='lxml'):
    return {'id': 'tvbs', 'domains': list(TVBS_HOSTS), 'enabled': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-15',
            'evidence_url': 'https://www.tvbs.com.tw/policies/copyright',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Locally known unchanged public original reports, private noncommercial reading only; no training/export'},
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'capacity_max_parallelism': 2, 'html_parser': parser,
        'embedded_article_layout': 'tvbs_visible_article_v1', 'article_path_pattern': TVBS_PATH_PATTERN,
        'article_selectors': [TVBS_BODY_SELECTOR], 'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'feeds': [], 'sitemaps': [], 'wordpress_apis': [], 'fulltext_feeds': []}


def config(parser='lxml'):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    return {**cfg, 'providers': [recipe(parser)]}


def policy(parser='lxml'):
    return PolicyRegistry(config(parser), base=Path.cwd()).sources['tvbs']


def page(host='news.tvbs.com.tw', parser='lxml'):
    url = URLS[TVBS_HOSTS.index(host)]
    published = '2026-10-14T08:29:26.000Z'
    modified = '2026-10-14T08:45:53.000Z'
    author = {'@type': 'Person', 'name': '研究記者'}
    publisher = {'@id': 'https://' + host + '/#organization'}
    caption = '<figure><img src="https://media.invalid/original.jpg"><figcaption>獨立圖片原出處</figcaption></figure>'
    prose = '<p>' + PROSE + '</p><h2>原始小標題</h2><p>' + NOTICE + '</p>' + caption
    related = '<aside>更多新聞：<a href="/money/12">鄰篇不是正文</a></aside>'
    extra = ''
    suffix = ''
    if host == TVBS_HOSTS[0]:
        labels = '<ul><li>記者：研究記者</li><li>發佈時間：2026.10.14 16:29</li><li>最後更新時間：2026.10.14 16:45</li></ul>'
        body = '<main><div><div><header><h1>' + TITLE + '</h1><div>' + labels + '</div><astro-island>' + labels + '</astro-island></header>'
        body += '<main><div class="article-editor-content"><div><a href="https://roundtable-tech-zh.vive.com/query?path=%2Fmoney%2F4035845&from=t">到VIVE科技圓桌討論，聽聽專家怎麼說</a></div>'
        body += prose + related + '<div class="inline-pc-ad-wrapper"><div id="news_pc_read_in1"></div></div></div></main></div></div></main>'
        extra = '<script type="application/ld+json">' + json.dumps({'@type': 'NewsMediaOrganization',
            '@id': 'https://' + host + '/#organization', 'name': 'TVBS新聞網', 'url': 'https://' + host + '/'}) + '</script>'
    elif host == TVBS_HOSTS[1]:
        author.update(jobTitle='記者', url='https://' + host + '/author/reporter')
        publisher.update({'@type': 'NewsMediaOrganization', 'name': '地球黃金線'})
        suffix = ' | 地球黃金線'
        body = '<main><article><div><h1>' + TITLE + '</h1><a href="/author/reporter">記者 研究記者</a><time datetime="' + published + '">2026/10/14</time></div>'
        body += '<div class="items-start"><div class="gap-article-section-gap">' + prose
        body += '<div><p>延伸閱讀</p><a href="/life/12">鄰篇不是正文</a></div><div><a href="/tag/test">標籤不是正文</a></div><div></div></div></div></article></main>'
    else:
        author['@id'] = 'https://' + host + '/author/reporter'
        publisher = {'@type': 'Organization', 'name': '食尚玩家'}
        published, modified = '2026-07-06T16:20:10+08:00', '2026-07-07T09:56:55+08:00'
        suffix = ' | 食尚玩家'
        body = '<article id="article-content"><header><h1>' + TITLE + '</h1><div class="mb-4">全台｜撰文 研究記者｜圖片來源：店家<a><svg aria-hidden="true"></svg></a></div>'
        body += '<div><a href="/author/reporter">《食尚玩家》資深記者 研究記者</a><img aria-hidden="true"></div></header>' + prose
        body += '<section id="coupon_block">本週優惠券，領起來<a href="/offers/12">優惠不是正文</a></section><section>文章目錄<a href="#A01">原始小標題</a></section>'
        body += '<section>達人推薦<a href="/player/12">原出處</a></section><div>廣告 - 內文未完請往下捲動<div id="supertaste_pc_read_in1"></div></div>'
        body += '<div>店家營業時間：11:00～22:00<a href="https://supertaste.tvbs.com.tw/infocard/12">店家卡片</a></div>'
        body += '<div id="article-end-marker"></div><section>鄰篇不是正文</section></article>'
    schema = {'@type': 'Article' if host == TVBS_HOSTS[2] else 'NewsArticle',
        '@id': url if host == TVBS_HOSTS[2] else url + '#article',
        'mainEntityOfPage': {'@type': 'WebPage', '@id': url}, 'headline': TITLE,
        'author': [author] if host == TVBS_HOSTS[1] else author, 'publisher': publisher,
        'datePublished': published, 'dateModified': modified, 'description': '摘要不得冒充正文'}
    if host == TVBS_HOSTS[1]:
        schema['url'] = url
    return BeautifulSoup('<html><head><meta charset="utf-8"><link rel="canonical" href="' + url + '">' +
        '<meta property="og:url" content="' + url + '"><meta property="og:title" content="' + TITLE + suffix + '">' +
        '<meta property="article:published_time" content="' + published + '"><meta property="article:modified_time" content="' + modified + '">' +
        '<script id="report" type="application/ld+json">' + json.dumps(schema, ensure_ascii=False) + '</script>' + extra +
        '</head><body>' + body + '<aside><h1>別篇標題</h1><p>鄰篇不是正文</p><time>2026/10/15</time></aside>' +
        '<div hidden id="S:0">隱藏 React 備份不能當本文</div><script>摘要或 script 不能當正文</script></body></html>', parser)


def extract(soup, url, parser='lxml'):
    return extract_article(str(soup).encode(), policy(parser), page_url=url)


def update_schema(soup, **changes):
    node = soup.select_one('script#report')
    value = json.loads(node.get_text())
    value.update(changes)
    node.string = json.dumps(value, ensure_ascii=False)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS, URLS)))
def test_three_whole_native_reports_raw_values_risk_notices_and_clocks(host, url, parser):
    soup = page(host, parser)
    before = str(soup)
    result = extract(soup, url, parser)
    assert PROSE in result['body'] and NOTICE in result['body'] and '原始小標題' in result['body']
    assert not any(s in result['body'] for s in ('鄰篇不是正文', '別篇標題', '摘要不得冒充正文', '摘要或 script 不能當正文', '隱藏 React', '記者',
        '獨立圖片原出處', '到VIVE', '優惠不是正文', '文章目錄', '店家卡片', '標籤不是正文'))
    assert result['authors_json'] == '["研究記者"]'
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert result['published_at_raw'] == ('2026-07-06T16:20:10+08:00' if host == TVBS_HOSTS[2] else '2026-10-14T08:29:26.000Z')
    assert result['modified_at_raw'] == ('2026-07-07T09:56:55+08:00' if host == TVBS_HOSTS[2] else '2026-10-14T08:45:53.000Z')
    assert json.loads(result['verified_image_captions_json']) == ['獨立圖片原出處']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert str(soup) == before


@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS, URLS)))
@pytest.mark.parametrize('change', ['canonical', 'og_url', 'schema_id', 'schema_page', 'second_article',
    'title', 'both_declared_titles', 'schema_title_missing', 'paid', 'unknown_paid', 'author_absent', 'author_empty',
    'author_competing', 'publisher', 'missing_pub', 'missing_mod', 'naive_pub', 'invalid_date',
    'timezone', 'meta_pub', 'meta_mod', 'revision_before', 'body_hidden', 'prose_hidden',
    'caption_hidden', 'script', 'nested_article', 'third_party_credit'])
def test_missing_ambiguous_hidden_paid_or_foreign_evidence_is_not_accepted(host, url, change):
    soup = page(host)
    body = soup.select_one('.article-editor-content, .gap-article-section-gap, article#article-content')
    if change == 'canonical':
        soup.select_one('link[rel=canonical]')['href'] = url + '0'
    elif change == 'og_url':
        soup.select_one('meta[property="og:url"]')['content'] = URLS[(TVBS_HOSTS.index(host) + 1) % 3]
    elif change == 'schema_id':
        update_schema(soup, **{'@id': url + '0'})
    elif change == 'schema_page':
        update_schema(soup, mainEntityOfPage={'@type': 'WebPage', '@id': url + '0'})
    elif change == 'second_article':
        soup.head.append(deepcopy(soup.select_one('script#report')))
    elif change == 'title':
        soup.select_one('h1').string = '別篇'
    elif change == 'both_declared_titles':
        soup.select_one('meta[property="og:title"]')['content'] = '別篇'
        update_schema(soup, headline='別篇')
    elif change == 'schema_title_missing':
        update_schema(soup, headline=None)
    elif change in {'paid', 'unknown_paid'}:
        update_schema(soup, isAccessibleForFree=False if change == 'paid' else 'unknown')
    elif change.startswith('author_'):
        update_schema(soup, author=None if change == 'author_absent' else {'@type': 'Person', 'name': ''}
            if change == 'author_empty' else [{'@type': 'Person', 'name': '研究記者'}] * 2)
    elif change == 'publisher':
        update_schema(soup, publisher={'@type': 'Organization', 'name': '其他媒體'})
    elif change in {'missing_pub', 'missing_mod', 'naive_pub', 'invalid_date', 'timezone', 'revision_before'}:
        field = 'dateModified' if change in {'missing_mod', 'revision_before'} else 'datePublished'
        value = {'missing_pub': None, 'missing_mod': None, 'naive_pub': '2026-10-14T08:29:26',
            'invalid_date': '2026-02-30T08:29:26.000Z', 'timezone': '2026-10-14T08:29:26+09:00',
            'revision_before': '2020-10-14T08:29:26.000Z'}[change]
        update_schema(soup, **{field: value})
        if value is not None:
            soup.select_one('meta[property="article:' + ('modified_time' if field == 'dateModified' else 'published_time') + '"]')['content'] = value
    elif change in {'meta_pub', 'meta_mod'}:
        soup.select_one('meta[property="article:' + ('published_time' if change == 'meta_pub' else 'modified_time') + '"]')['content'] = '2026-10-15T08:29:26.000Z'
    elif change == 'body_hidden':
        body['hidden'] = ''
    elif change == 'prose_hidden':
        body.find('p', recursive=False)['style'] = 'display: none'
    elif change == 'caption_hidden':
        body.select_one('figcaption')['aria-hidden'] = 'true'
    elif change in {'script', 'nested_article'}:
        added = soup.new_tag('script' if change == 'script' else 'article')
        marker = body.select_one(':scope > #article-end-marker')
        marker.insert_before(added) if marker else body.append(added)
    elif change == 'third_party_credit':
        body.find('p', recursive=False).string = '中央社記者測試／台北報導 ' + PROSE
    with pytest.raises(NewsError):
        extract(soup, url)


@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS, URLS)))
@pytest.mark.parametrize('variant', ['schema_only', 'social_only'])
def test_source_SEO_title_variants_need_one_native_title_anchor_and_keep_all_originals(host, url, variant):
    soup = page(host)
    if variant == 'schema_only':
        update_schema(soup, headline='來源 schema SEO 標題原值')
    else:
        soup.select_one('meta[property="og:title"]')['content'] = '來源 OG 社群標題原值'
    result = extract(soup, url)
    assert result['title'] == TITLE and PROSE in result['body']
    notices = {row['kind']: row['text'] for row in json.loads(result['verified_article_notices_json'])}
    assert notices['native_visible_headline'] == TITLE
    assert notices['native_schema_headline'] == ('來源 schema SEO 標題原值' if variant == 'schema_only' else TITLE)
    assert notices['native_OG_headline'].startswith('來源 OG 社群標題原值' if variant == 'social_only' else TITLE)
    assert result['declared_canonical_url'] == url and not result['historical_point_in_time']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS[:2], URLS[:2])))
def test_own_native_editor_is_not_an_unknown_or_syndicated_credit(host, url, parser):
    soup = page(host, parser)
    if host == TVBS_HOSTS[0]:
        for label in soup.select('header li'):
            if label.get_text().startswith('記者：'):
                label.string = '編輯：研究記者'
    else:
        update_schema(soup, author=[{'@type': 'Person', 'name': '研究記者', 'jobTitle': '編輯',
            'url': 'https://' + host + '/author/reporter'}])
        soup.select_one('a[href^="/author/"]').string = '編輯 研究記者'
    result = extract(soup, url, parser)
    assert PROSE in result['body'] and NOTICE in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert any(n['kind'] == 'native_editor_credit' and '研究記者' in n['text'] for n in notices)
    assert not any(n['kind'] == 'native_reporter_credit' for n in notices)
    assert json.loads(result['author_attribution_detail_json'])['native_author_name'] == '研究記者'
    assert not result['historical_point_in_time'] and result['authors_json'] == '["研究記者"]'


@pytest.mark.parametrize('change', ['competing_role', 'unknown_role', 'syndicated_editor'])
@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS[:2], URLS[:2])))
def test_editor_role_still_requires_exact_matching_original_credit(host, url, change):
    soup = page(host)
    if host == TVBS_HOSTS[0]:
        labels = soup.select('header li')
        if change == 'competing_role':
            labels[3].string = '編輯：研究記者'
        elif change == 'unknown_role':
            labels[0].string = '轉載：研究記者'; labels[3].string = '轉載：研究記者'
        else:
            update_schema(soup, author={'@type': 'Person', 'name': '中央社'})
            labels[0].string = '編輯：中央社'; labels[3].string = '編輯：中央社'
    else:
        role = '轉載' if change == 'unknown_role' else '編輯'
        name = '中央社' if change == 'syndicated_editor' else '研究記者'
        update_schema(soup, author=[{'@type': 'Person', 'name': name, 'jobTitle': role,
            'url': 'https://' + host + '/author/reporter'}])
        soup.select_one('a[href^="/author/"]').string = ('記者' if change == 'competing_role' else role) + ' ' + name
    with pytest.raises(NewsError):
        extract(soup, url)


@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS, URLS)))
def test_parenthetical_wire_report_is_not_own_text_but_quote_and_image_credit_are_not_body_roles(host, url):
    soup = page(host)
    body = soup.select_one('.article-editor-content, .gap-article-section-gap, article#article-content')
    body.find('p', recursive=False).string = '（中央社台北14日電）' + PROSE
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, url)
    body.find('p', recursive=False).string = '本台記者查證並引用中央社報導。' + PROSE
    body.select_one('figcaption').string = '圖片原出處：中央社'
    result = extract(soup, url)
    assert '引用中央社報導' in result['body'] and PROSE in result['body']
    assert '圖片原出處：中央社' in json.loads(result['verified_image_captions_json'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_cars_old_native_linked_reading_UI_is_not_prose_or_neighboring_story(parser):
    soup = page(TVBS_HOSTS[1], parser)
    body = soup.select_one('.gap-article-section-gap')
    legacy = BeautifulSoup('<p class="article-paragraph"><b>延伸閱讀：</b></p><ul>'
        '<li><span><a href="https://cars.tvbs.com.tw/life/284654"><b>鄰篇不是正文</b></a></span></li>'
        '<li><span><a href="https://cars.tvbs.com.tw/life/282156"><b>另一篇不是正文</b></a></span></li></ul>', parser)
    for node in list((legacy.body or legacy).find_all(recursive=False)):
        body.append(node.extract())
    paragraph = soup.new_tag('p', attrs={'class': 'article-paragraph'})
    paragraph.string = '這一段真正內文提到延伸閱讀：以及原始資料。'
    body.append(paragraph)
    result = extract(soup, URLS[1], parser)
    assert PROSE in result['body'] and NOTICE in result['body']
    assert paragraph.get_text() in result['body']
    assert '鄰篇不是正文' not in result['body'] and '另一篇不是正文' not in result['body']


@pytest.mark.parametrize('change', ['foreign_link', 'current_link', 'data_not_link', 'unlabelled_data'])
def test_cars_further_reading_shape_cannot_erase_unknown_prose_or_actual_data_lists(change):
    soup = page(TVBS_HOSTS[1])
    body = soup.select_one('.gap-article-section-gap')
    label = soup.new_tag('p', attrs={'class': 'article-paragraph'})
    bold = soup.new_tag('b'); bold.string = '延伸閱讀：'; label.append(bold)
    ul = soup.new_tag('ul'); item = soup.new_tag('li'); anchor = soup.new_tag('a', href=(
        'https://foreign.invalid/life/284654' if change == 'foreign_link' else URLS[1]
        if change == 'current_link' else 'https://cars.tvbs.com.tw/life/284654'))
    anchor.string = '實際資料123456股'; item.append(anchor); ul.append(item)
    if change == 'data_not_link':
        item.append(' 非連結的原始資料-12.5%')
    elif change == 'unlabelled_data':
        bold.string = '原始分組統計：'
    body.append(label); body.append(ul)
    result = extract(soup, URLS[1])
    assert label.get_text() in result['body'] and '\n'.join(item.stripped_strings) in result['body']


@pytest.mark.parametrize('change', ['missing_publication', 'competing_publication', 'competing_revision',
    'hidden_clock_text', 'hidden_author_text', 'unlabelled_clock', 'external_byline'])
def test_news_native_responsive_clocks_and_reporter_must_agree(change):
    soup = page()
    labels = soup.select('header li')
    if change == 'missing_publication':
        labels[1].decompose(); labels[4].decompose()
    elif change == 'competing_publication':
        labels[4].string = '發佈時間：2026.10.14 16:30'
    elif change == 'competing_revision':
        labels[5].string = '最後更新時間：2026.10.14 16:46'
    elif change in {'hidden_clock_text', 'hidden_author_text'}:
        label = labels[1] if change == 'hidden_clock_text' else labels[0]
        span = soup.new_tag('span', hidden=''); span.string = label.get_text(); label.clear(); label.append(span)
    elif change == 'unlabelled_clock':
        labels[1].string = '2026.10.14 16:29'
    else:
        labels[0].string = '編輯：中央社'; labels[3].string = '編輯：中央社'
    with pytest.raises(NewsError):
        extract(soup, URLS[0])


def test_food_native_credit_not_empty_decorative_svg_and_no_venue_hours_as_release():
    soup = page(TVBS_HOSTS[2])
    result = extract(soup, URLS[2])
    assert 'native_visible_release_clock_not_present' in result['published_time_basis']
    notices = json.loads(result['verified_article_notices_json'])
    assert any(n['kind'] == 'native_venue_information_card' and '11:00～22:00' in n['text'] for n in notices)
    native = soup.select_one('header > div.mb-4')
    hidden = soup.new_tag('span', hidden=''); hidden.string = native.get_text(' ', strip=True)
    native.clear(); native.append(hidden)
    with pytest.raises(NewsError):
        extract(soup, URLS[2])


@pytest.mark.parametrize('host,url', list(zip(TVBS_HOSTS, URLS)))
def test_native_account_and_role_cannot_be_borrowed_from_other_story(host, url):
    soup = page(host)
    if host == TVBS_HOSTS[0]:
        for n in soup.select('header li'):
            if n.get_text().startswith('記者'):
                n.string = '記者：別人'
    else:
        soup.select_one('a[href^="/author/"]')['href'] = '/author/other'
    with pytest.raises(NewsError):
        extract(soup, url)


@pytest.mark.parametrize('field,value', [('domains', ['tvbs.com.tw']), ('article_path_pattern', '^/.*'),
    ('article_selectors', ['body']), ('require_author_attribution', False), ('require_configured_container', False),
    ('exclude_selectors', ['p']), ('feeds', ['https://news.tvbs.com.tw/rss']), ('sitemaps', ['https://news.tvbs.com.tw/sitemap.xml']),
    ('wordpress_apis', ['https://news.tvbs.com.tw/wp-json/wp/v2/posts']), ('private_known_url_scope', ''),
    ('acquisition_mode', 'finite')])
def test_native_layout_does_not_expand_source_permission_or_discovery(field, value):
    cfg = config(); cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


def test_post_cutover_installed_contract_accepts_only_the_reviewed_candidate(monkeypatch):
    import test_news_fulltext as shared
    cfg, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(cfg)
    candidate['providers'] = [recipe() if p['id'] == 'tvbs' else p for p in cfg['providers']]
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('url,expected', [(URLS[0], ('news.tvbs.com.tw', '/money/4035845')),
    (URLS[0] + '?from=money_content_pack', ('news.tvbs.com.tw', '/money/4035845')),
    (URLS[0] + '?article=other', None), (URLS[0] + '?from=a&from=b', None),
    ('https://news.tvbs.com.tw/ttalk/detail/life/13964', None), ('https://other.tvbs.com.tw/money/4035845', None)])
def test_literal_identity_admission_does_not_invent_alternate_requests(url, expected):
    assert tvbs_request_identity(url) == expected


@pytest.mark.parametrize('request_url', [URLS[0], URLS[0] + '?from=money_content_pack', URLS[0].replace('https:', 'http:')])
def test_ordinary_fetch_keeps_original_request_and_source_verified_response(request_url, tmp_path):
    cfg = config()
    response_url = request_url.replace('http:', 'https:')
    payload = str(page()).encode()
    responses = {response_url: FakeResponse(payload),
        'https://news.tvbs.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'http://news.tvbs.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')}
    if response_url != request_url:
        responses[request_url] = FakeResponse(status=308, headers={'Location': response_url})
    http, session = http_fixture(cfg, tmp_path, responses)
    q = NewsQueue(tmp_path, http.registry)
    try:
        q.add_articles([{'url': request_url}])
        row = q.claim_url(request_url, 300)
        result = fetch_article(http, row)
        assert result['requested_url'] == request_url and result['final_url'] == response_url
        assert result['declared_canonical_url'] == URLS[0] and PROSE in result['body']
        assert [call[0] for call in session.calls if not call[0].endswith('/robots.txt')] == (
            [request_url, response_url] if response_url != request_url else [request_url])
        q.accept([result])
        q.close()
        q = NewsQueue(tmp_path, http.registry)
        assert q.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 1
        assert q.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert not q.db.execute('SELECT 1 FROM discovery').fetchone()
        assert verify_bodies(tmp_path)['accepted']
    finally:
        q.close(); http.close()


@pytest.mark.parametrize('redirect', [URLS[0] + '0', URLS[0].replace('/money/', '/life/'), URLS[1]])
def test_transport_redirect_cannot_substitute_a_different_report(redirect, tmp_path):
    http, session = http_fixture(config(), tmp_path, {URLS[0]: FakeResponse(status=308, headers={'Location': redirect}),
        redirect: FakeResponse(str(page()).encode()),
        'https://news.tvbs.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://cars.tvbs.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')})
    try:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            fetch_article(http, {'url': URLS[0], 'id': b'\x01' * 16})
        assert [call[0] for call in session.calls if not call[0].endswith('/robots.txt')] == [URLS[0], redirect]
    finally:
        http.close()
