"""Observed campus-news layout; no made-up clocks, broad hosts or media scope."""
from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_nccu import NCCU_REVIEWED_URL, nccu_visible_news
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


TITLE = '原始校訊數值 76比57、-12.5 與 0.75'
TEXT = '這是本篇完整原始正文；不得拿頁尾文字、縮圖說明或鄰近文章取代。' * 5
CONFIG = Path('configs/news/probe_nccu_20261012.json')


def page(*, title=TITLE, head_title=None, date='2021-03-20', unit='秘書處', credit='【校訊記者林亭報導】',
         article_id='9212', stat_id='9212', body=TEXT, attrs='', panel_attrs='', heading_attrs='', extra=''):
    return BeautifulSoup(f'''<html><head><meta charset="utf-8"><title>{head_title or title + ' - 國立政治大學'}</title></head>
    <body><div class="meditor">另一篇作者與2026-10-12假日期</div>
    <div class="module module-detail"><div class="module-inner"><section class="mb">
    <div class="mpgtitle"><h2 class="hdline" {heading_attrs}>{title}</h2></div><div class="mcont">
    <div id="PtDetail_{article_id}_ImageList1">SSU第三方圖說不得混入</div>
    <div id="PtDetail_{article_id}_ImageList2">SSU第三方圖說不得混入</div>
    <div class="mpgdetail"><div class="ptinfoproperty" {panel_attrs}>
    <span class="ptinfoproperty_date">日期 :<span>{date}</span></span>
    <span class="ptinfoproperty_group">單位 :<span>{unit}</span></span></div>
    <div class="meditor" {attrs}>{credit}<br>{body}<br><br>本篇最後原始段落。</div></div></div></section></div></div>
    <div class="module-misc"><div class="artctrl"><div class="PtStatistic"><em>
    <i id="a76bf9793c641409f0b292afa6a63abbd_PtStatistic_{stat_id}"></i></em></div></div></div>
    {extra}<footer>2026-10-12網站更新日期與另一位作者</footer></body></html>''', 'lxml')


def test_current_native_text_release_date_and_reporter_are_distinct():
    result, scope = nccu_visible_news(page(), page_url=NCCU_REVIEWED_URL)
    assert result['headline'] == TITLE
    assert result['author'] == [{'name': '林亭'}]
    assert result['datePublished'] == '2021-03-20'
    assert result['published_time_basis'] == 'publisher_declared_date_only; no_intraday_time_inferred'
    assert 'dateModified' not in result and 'articleBody' not in result
    assert '秘書處' in result['author_attribution_detail_json']
    assert scope.select_one('.mpgdetail > .meditor').get_text().startswith('【校訊記者林亭報導】')


@pytest.mark.parametrize('changes', [
    {'head_title': '另一篇 - 國立政治大學'}, {'article_id': '9213'}, {'stat_id': '9213'},
    {'attrs': 'hidden'}, {'attrs': 'aria-hidden="true"'}, {'attrs': 'style="display: none"'},
    {'panel_attrs': 'hidden'}, {'heading_attrs': 'hidden'},
    {'body': '<span hidden>' + TEXT + '</span>'}, {'body': '<p>' + TEXT + '</p>'},
    {'extra': '<link rel="canonical" href="https://elsewhere.test/another">'},
    {'extra': '<meta property="og:url" content="https://elsewhere.test/another">'},
    {'extra': '<script type="application/ld+json">{"articleBody":"不可借用"}</script>'},
])
def test_unreviewed_or_hidden_representation_not_accepted(changes):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        nccu_visible_news(page(**changes), page_url=NCCU_REVIEWED_URL)


@pytest.mark.parametrize('changes', [
    {'unit': '外部媒體'}, {'credit': '【中央社記者報導】'}, {'credit': '【校訊記者其他人報導】'},
    {'credit': ''}, {'credit': '林亭'},
])
def test_site_host_and_another_byline_are_not_source_permission(changes):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        nccu_visible_news(page(**changes), page_url=NCCU_REVIEWED_URL)


@pytest.mark.parametrize('clock', ['2021-02-30', '2021-3-20', '2021-03-20T00:00:00+08:00', '更新:2021-03-20', ''])
def test_no_inferred_date_or_publication_precision(clock):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        nccu_visible_news(page(date=clock), page_url=NCCU_REVIEWED_URL)


@pytest.mark.parametrize('url', [None, NCCU_REVIEWED_URL + '#body', NCCU_REVIEWED_URL + '&other=1',
    NCCU_REVIEWED_URL.replace('https:', 'http:'), NCCU_REVIEWED_URL.replace('www.nccu.edu.tw', 'sub.nccu.edu.tw'),
    NCCU_REVIEWED_URL.replace('9212', '9213'), NCCU_REVIEWED_URL.replace(',', '%2C')])
def test_exact_known_url_only_no_guessed_alias(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        nccu_visible_news(page(), page_url=url)


@pytest.mark.parametrize('selector', ['.module-detail', '.ptinfoproperty_date', '.ptinfoproperty_group', '.mpgdetail > .meditor', '.PtStatistic'])
def test_duplicate_current_article_evidence_is_ambiguous(selector):
    soup = page()
    node = soup.select_one(selector)
    node.insert_after(deepcopy(node))
    with pytest.raises(NewsError):
        nccu_visible_news(soup, page_url=NCCU_REVIEWED_URL)


def test_shared_extractor_preserves_whole_text_and_precision_without_media():
    source = load_config(CONFIG)[1].require(NCCU_REVIEWED_URL)
    result = extract_article(str(page()), source, page_url=NCCU_REVIEWED_URL)
    assert result['title'] == TITLE
    assert TEXT in result['body'] and '本篇最後原始段落。' in result['body']
    assert result['body'].startswith('【校訊記者林亭報導】')
    for excluded in ('SSU', '另一篇作者', '網站更新日期', '2026-10-12'):
        assert excluded not in result['body']
    assert json.loads(result['authors_json']) == ['林亭']
    assert result['published_at_raw'] == '2021-03-20'
    assert result['modified_at_raw'] is None
    assert result['declared_canonical_url'] is None
    assert result['historical_point_in_time'] is False
    assert 'date_only' in result['published_time_basis']
    assert result['extraction_method'].startswith('css:')


@pytest.mark.parametrize('changes', [
    {'domains': ['nccu.edu.tw']}, {'domains': ['news.nccu.edu.tw']},
    {'id': 'another'}, {'acquisition_mode': 'continuous'},
    {'require_author_attribution': False}, {'require_configured_container': False},
    {'article_selectors': ['.meditor']}, {'exclude_selectors': ['.meditor']},
    {'article_path_pattern': '^/p/406-'},
    {'authorization': {'kind': 'open_license', 'license': 'CC-BY-4.0', 'scope': 'test',
                       'evidence_url': 'https://www.nccu.edu.tw/p/426-1000-30.php?Lang=zh-tw'}},
])
def test_shared_registry_rejects_unreviewed_broadening(tmp_path, changes):
    cfg = json.loads(CONFIG.read_bytes())
    cfg['providers'][0].update(changes)
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=tmp_path)
