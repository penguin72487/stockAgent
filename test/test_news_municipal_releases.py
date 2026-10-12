"""Native source identity, government organization roles and date precision."""
from copy import deepcopy
from html import escape
import json
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_taiwan_pages import taichung_visible_news, changhua_visible_news


CONFIG = Path('configs/news/probe_municipal_releases_20261012.json')
TW_URL = 'https://www.taichung.gov.tw/2197559/post'
CH_URL = 'https://www.chcg.gov.tw/ch2/newsdetail.aspx?bull_id=382888'
TITLE = '原始政策76億元、-12.5%、8454與550個工作機會'
TEXT = '完整原始敘事保留76億元、8454、-12.5%、550、0.75與2027年，不借用網站頁尾。' * 5


def taichung_page(*, parser='lxml', id='2197559', canonical_id=None, title=TITLE, ogtitle=None,
                  clock='2022-10-19', unit='台中市政府環境保護局', body=TEXT,
                  attrs='', panel_attrs='', head_attrs='', extra='', duplicate_date=False):
    canonical = f'https://www.taichung.gov.tw/8868/8872/9962/{canonical_id or id}'
    return BeautifulSoup(f'''<html><head><meta charset="utf-8">
    <link rel="canonical" href="{canonical}"><meta property="og:url" content="{canonical}">
    <meta property="og:title" content="{ogtitle or title}"></head><body>
    <h2>相鄰標題及中央社作者不是本篇署名</h2><main id="center">
    <header><h2 {head_attrs}>{title}</h2></header>
    <article id="cpArticle" class="cpArticle" {attrs}>
    <figure><img alt="另一段媒體數值不可當正文" src="/media/private.jpg"><figcaption>第三方圖說</figcaption></figure>
    <p>{body}</p><p>(10/19*17)*環保局</p><p>聯絡人:{unit} 萬科長</p><p>聯絡電話:04-22289111分機66401</p>
    </article><section class="pubInfo" {panel_attrs}><ul>
    <li>市府分類：<em>環保綠化</em></li><li>發布日期：<em>{clock}</em></li>
    {'<li>發布日期：<em>'+clock+'</em></li>' if duplicate_date else ''}
    <li>點閱次數：<em>1774</em></li></ul></section></main>{extra}
    <footer>最後修改日期2026-10-12，某位外部作者</footer></body></html>''', parser)


def taichung_og_only_page(*, parser='lxml'):
    soup = taichung_page(parser=parser)
    soup.select_one('link[rel~="canonical"]').decompose()
    pointers = (
        ('https://www.facebook.com/sharer.php', {'u': TW_URL}),
        ('https://twitter.com/share', {'text': TITLE, 'url': TW_URL}),
        ('https://social-plugins.line.me/lineit/share', {'url': TW_URL, 'text': TITLE, 'from': 'line_scheme'}),
    )
    markup = '<section class="friendly"><a href="/9962/Lpsimplelist">市政新聞</a></section>'
    markup += '<section class="share">' + ''.join(
        '<a href="' + escape(url + '?' + urlencode(params), quote=True) + '">分享</a>'
        for url, params in pointers) + '</section>'
    fragments = BeautifulSoup(markup, parser)
    for section in list(fragments.select('section')):
        soup.select_one('main#center').append(section.extract())
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_actual_og_only_variant_requires_native_breadcrumb_and_three_exact_share_ids(parser):
    soup = taichung_og_only_page(parser=parser)
    before = str(soup)
    _, registry, _ = load_config(CONFIG)
    result = extract_article(str(soup).encode(), registry.require(TW_URL), page_url=TW_URL)
    assert TEXT in result['body']
    assert 'three_declared_share_URL_witnesses' in result['article_identity_basis']
    assert 'canonical_not_invented' in result['article_identity_basis']
    assert soup.select('link[rel~="canonical"]') == [] and str(soup) == before


@pytest.mark.parametrize('mutation', [
    lambda s: s.select_one('section.share a').decompose(),
    lambda s: s.select_one('section.share').append(deepcopy(s.select_one('section.share a'))),
    lambda s: s.select_one('section.share a').__setitem__('href', 'https://www.facebook.com/sharer.php?u=' + TW_URL.replace('2197559', '2197560')),
    lambda s: s.select_one('section.share a').__setitem__('href', 'https://foreign.test/sharer.php?u=' + TW_URL),
    lambda s: s.select_one('section.share a').__setitem__('href', 'http://www.facebook.com/sharer.php?u=' + TW_URL),
    lambda s: s.select_one('section.share a').__setitem__('href', 'https://www.facebook.com/sharer.php?u=' + TW_URL + '&u=' + TW_URL),
    lambda s: s.select_one('section.share a').__setitem__('href', 'https://www.facebook.com/sharer.php?u=' + TW_URL + '#extra'),
    lambda s: s.select('section.share a')[1].__setitem__('href', 'https://twitter.com/share?' + urlencode({'url': TW_URL, 'text': '別篇文章'})),
    lambda s: s.select('section.share a')[2].__setitem__('href', 'https://social-plugins.line.me/lineit/share?' + urlencode({'url': TW_URL, 'text': TITLE, 'from': 'wrong'})),
    lambda s: s.select_one('section.share').__setitem__('hidden', ''),
    lambda s: s.select_one('section.friendly').__setitem__('style', 'display:none'),
    lambda s: s.select_one('section.friendly a').__setitem__('href', '/9963/Lpsimplelist'),
    lambda s: setattr(s.select_one('section.friendly a'), 'string', '不同類型'),
])
def test_og_only_variant_never_accepts_missing_competing_foreign_or_hidden_witness(mutation):
    soup = taichung_og_only_page()
    mutation(soup)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        taichung_visible_news(soup, page_url=TW_URL)


@pytest.mark.parametrize('raw,unit,person', [
    ('台中市政府', '台中市政府', '萬科長'),
    ('台中政府運動局', '台中政府運動局', '萬科長'),
    ('台中市中興地政事務所', '台中市中興地政事務所', '萬科長'),
    ('台中市大甲區戶政事務所', '台中市大甲區戶政事務所', '萬科長'),
    ('台中市大甲地政事務所', '台中市大甲地政事務所', '萬科長'),
])
def test_explicit_observed_office_names_and_source_typo_preserved_not_expanded(raw, unit, person):
    article, _ = taichung_visible_news(taichung_page(unit=raw), page_url=TW_URL)
    detail = json.loads(article['author_attribution_detail_json'])
    assert article['author'] == [{'@type': 'Organization', 'name': unit}]
    assert detail['release_contact_organization_raw'] == unit
    assert detail['source_contact_person_raw'] == person
    assert not detail['named_author_invented']


def test_contact_person_without_separator_is_never_part_of_organization_name():
    soup = taichung_page()
    paragraphs = soup.select('article#cpArticle > p')
    paragraphs[-2].string = '聯絡人:台中市政府民政局張小姐'
    article, _ = taichung_visible_news(soup, page_url=TW_URL)
    detail = json.loads(article['author_attribution_detail_json'])
    assert article['author'] == [{'@type': 'Organization', 'name': '台中市政府民政局'}]
    assert detail['release_contact_raw'] == '台中市政府民政局張小姐'
    assert detail['source_contact_person_raw'] == '張小姐'


def changhua_page(*, parser='lxml', id='382888', form_id=None, breadcrumb_id=None, print_id=None,
                  title=TITLE, ogtitle=None, body=TEXT, unit='經濟暨綠能發展處',
                  period='2023/11/08~2023/12/08', header_day='2023/11/08', marker='(歷史訊息)',
                  attrs='', panel_attrs='', head_attrs='', extra=''):
    return BeautifulSoup(f'''<html><head><meta charset="utf-8"><meta property="og:title" content="{ogtitle or title}"></head>
    <body><a class="active" href="newsdetail.aspx?bull_id={breadcrumb_id or id}">{title}</a>
    <form id="form1" action="./newsdetail.aspx?bull_id={form_id or id}">
    <div id="print_parts"><div class="ipdb-title-block"><div class="ipdb-title" {head_attrs}>
    <span style="color:red;">{marker}</span>{title}</div><div class="d-v"><span class="date"><span>{header_day}</span></span></div>
    <a href="print_all.aspx?bull_id={print_id or id}">友善列印</a></div>
    <div class="active-detail-list-block" {panel_attrs}><ul>
    <li><div class="t">發布單位</div><div class="c">{unit}</div></li>
    <li><div class="t">發布日期</div><div class="c"><span>{period}</span></div></li>
    <li><div class="t">聯絡資訊</div><div class="c">洪科長7531461</div></li></ul></div>
    <article class="content" {attrs}>{body}<br>本篇最後完整數值540萬件。</article>
    <div class="active-detail-list-block introduce"><ul></ul></div></div></form>{extra}
    <footer>最後修改2026-10-12，外部作者</footer></body></html>''', parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('unit', ['台中市政府環境保護局', '台中市政府文化局表演藝術科', '台中市政府農業局', '台中市政府研考會'])
@pytest.mark.parametrize('url', [TW_URL, 'https://www.taichung.gov.tw/8868/8872/9962/2197559'])
def test_taichung_exact_release_prose_organization_date_only_and_no_contacts(parser, unit, url):
    _, registry, _ = load_config(CONFIG)
    soup = taichung_page(parser=parser, unit=unit)
    result = extract_article(str(soup).encode(), registry.sources['taichung_gov'], page_url=url)
    assert TEXT in result['body'] and '(10/19*17)*環保局' in result['body']
    assert all(value not in result['body'] for value in ['萬科長', '22289111', '聯絡人:', '第三方圖說', '最後修改'])
    assert result['authors_json'] == json.dumps([unit], ensure_ascii=False)
    assert result['published_at_raw'] == '2022-10-19' and result['modified_at_raw'] is None
    assert 'no_intraday_time_or_timezone_inferred' in result['published_time_basis']
    assert 'not_named_reporter' in result['author_attribution_detail_json']
    assert soup.select_one('#cpArticle').get_text().find('聯絡人:') >= 0  # Original parsed/source tree unchanged.


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_changhua_full_body_date_and_period_retained_without_reporter_or_expiry_invention(parser):
    _, registry, _ = load_config(CONFIG)
    result = extract_article(str(changhua_page(parser=parser)).encode(), registry.sources['changhua_gov'], page_url=CH_URL)
    assert TEXT in result['body'] and '540萬件' in result['body']
    assert all(value not in result['body'] for value in ['洪科長', '7531461', '歷史訊息', '最後修改'])
    assert result['authors_json'] == '["經濟暨綠能發展處"]'
    assert result['published_at_raw'] == '2023/11/08' and result['modified_at_raw'] is None
    assert json.loads(result['verified_article_notices_json'])[0]['text'] == '2023/11/08~2023/12/08'
    assert json.loads(result['author_attribution_detail_json'])['named_author_invented'] is False


@pytest.mark.parametrize('changes', [
    {'canonical_id': '2197560'}, {'ogtitle': '相鄰標題'}, {'attrs': 'hidden'},
    {'panel_attrs': 'style="display: none"'}, {'head_attrs': 'aria-hidden="true"'},
    {'body': '<span hidden>' + TEXT + '</span>'}, {'duplicate_date': True},
    {'extra': '<link rel="canonical" href="https://other.test/a">'},
])
def test_taichung_different_identity_or_hidden_publication_is_not_body(changes):
    with pytest.raises(NewsError):
        taichung_visible_news(taichung_page(**changes), page_url=TW_URL)


@pytest.mark.parametrize('unit', ['外部媒體', '台中市政府民政局網紅', '環保局', '中央社記者', ''])
def test_taichung_only_explicit_government_contact_organization_not_an_inferred_name(unit):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        taichung_visible_news(taichung_page(unit=unit), page_url=TW_URL)


@pytest.mark.parametrize('clock', ['2022-02-30', '2022-10-19T00:00:00+08:00', '2022-1-19', '2022/10/19', ''])
def test_taichung_unknown_precision_never_backdated_or_taken_from_footer(clock):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        taichung_visible_news(taichung_page(clock=clock), page_url=TW_URL)


@pytest.mark.parametrize('url', [TW_URL+'?a=1', TW_URL.replace('https:', 'http:'),
    TW_URL.replace('www.taichung.gov.tw', 'news.taichung.gov.tw'), TW_URL.replace('/2197559/', '/2197560/'),
    'https://www.taichung.gov.tw/8868/8872/9971/2197559'])
def test_taichung_only_the_reviewed_same_news_namespace(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        taichung_visible_news(taichung_page(), page_url=url)


@pytest.mark.parametrize('changes', [
    {'form_id': '382889'}, {'breadcrumb_id': '382889'}, {'print_id': '382889'},
    {'ogtitle': '另一篇原文'}, {'marker': '會員文章'}, {'attrs': 'hidden'},
    {'panel_attrs': 'aria-hidden="true"'}, {'head_attrs': 'style="visibility: hidden"'},
    {'body': '<span hidden>' + TEXT + '</span>'},
    {'extra': '<meta property="og:url" content="https://another.test/a">'},
    {'extra': '<script type="application/ld+json">{"articleBody":"假正文"}</script>'},
])
def test_changhua_native_ID_header_scope_and_visible_body_must_match(changes):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        changhua_visible_news(changhua_page(**changes), page_url=CH_URL)


@pytest.mark.parametrize('period,header', [
    ('2023/11/08~2023/12/08', '2023/11/09'), ('2023/11/08~2023/10/08', '2023/11/08'),
    ('2023/02/30~2023/12/08', '2023/02/30'), ('2023/11/08', '2023/11/08'),
    ('2023/11/08 00:00:00~2023/12/08', '2023/11/08'), ('2023/11/8~2023/12/08', '2023/11/8'),
])
def test_changhua_period_is_not_an_approximate_or_invented_publication_clock(period, header):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        changhua_visible_news(changhua_page(period=period, header_day=header), page_url=CH_URL)


@pytest.mark.parametrize('unit', ['中央社', '新聞媒體', '未審查部門', '', '経濟暨綠能發展處'])
def test_changhua_other_byline_or_departments_need_independent_scope_review(unit):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        changhua_visible_news(changhua_page(unit=unit), page_url=CH_URL)


@pytest.mark.parametrize('url', [CH_URL+'&other=1', CH_URL+'&bull_id=382888',
    CH_URL.replace('https:', 'http:'), CH_URL.replace('www.chcg.gov.tw', 'tourism.chcg.gov.tw'),
    CH_URL.replace('382888', '382889')])
def test_changhua_only_exact_main_host_native_request_identity(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        changhua_visible_news(changhua_page(), page_url=url)


@pytest.mark.parametrize('provider', ['taichung_gov', 'changhua_gov'])
@pytest.mark.parametrize('change', [{'id': 'generic'}, {'domains': ['other.gov.tw']},
    {'require_author_attribution': False}, {'require_configured_container': False},
    {'article_selectors': ['body']}, {'acquisition_mode': 'continuous'},
    {'authorization': {'kind': 'personal_research', 'evidence_url': 'https://elsewhere.test/'}}])
def test_registered_native_adapter_is_not_a_generic_permission_bypass(provider, change, tmp_path):
    cfg, _, _ = load_config(CONFIG)
    cfg = deepcopy(cfg)
    next(entry for entry in cfg['providers'] if entry['id'] == provider).update(change)
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(cfg, base=tmp_path)
