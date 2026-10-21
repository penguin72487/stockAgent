"""Observed calendar-day display uses real aware clocks, never fake midnight."""
from datetime import datetime
import json

from bs4 import BeautifulSoup
from bs4.element import Script
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_candidates import videoland_candidate_source
from downloader.news_fulltext_videoland import _article_clocks, videoland_visible_article, _decode_schema
from test_news_videoland_original import (
    URL, CREDIT, PROSE, CAPTION, COPYRIGHT, page, versioned_config, description_defect_page,
)

RELEASE = '2026-06-29T09:51:37+08:00'
REVISION = '2026-06-29T10:02:09+08:00'


def change_schema(soup, **fields):
    script = soup.find('script', type='application/ld+json')
    declared, _ = _decode_schema(script.get_text(), allow_description_lf=True)
    declared.update(fields)
    script.clear()
    script.append(Script(json.dumps(declared, ensure_ascii=False)))


def date_only(soup):
    change_schema(soup, datePublished=RELEASE, dateModified=REVISION)
    soup.select_one('meta[property="article:published_time"]')['content'] = RELEASE
    soup.select_one('meta[property="article:modified_time"]')['content'] = REVISION
    soup.select_one('#NewsRealDate').string = '2026/6/29'
    soup.select_one('#NewsModDate').decompose()


def soup_for(engine='lxml', change=None):
    def transform(soup):
        date_only(soup)
        if change:
            change(soup)
    return BeautifulSoup(description_defect_page(transform, absent_body=True), engine)


def clocks(soup, allow=True):
    declared, _ = _decode_schema(soup.find('script', type='application/ld+json').get_text(), allow_description_lf=True)
    return _article_clocks(soup, soup.select_one('header'), declared, allow_date_only=allow)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_offline_candidate_preserves_actual_seconds_and_absent_native_update(engine):
    article, body = videoland_visible_article(soup_for(engine), page_url=URL, adapter_contract_version=5)
    assert article['datePublished'] == RELEASE and article['dateModified'] == REVISION
    basis = article['published_time_basis']
    assert 'visible_release_calendar_day' in basis and 'native_visible_update_absent' in basis
    assert 'not_observed_historical_PIT' in basis and 'no_midnight_or_update_invention' in basis
    assert body.get_text().count(PROSE) == 1 and body.get_text().count(CAPTION) == 1
    assert COPYRIGHT in article['verified_article_notices_json']
    assert article['author'] == [{'@type':'Person','name':CREDIT}]
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        videoland_visible_article(soup_for(engine), page_url=URL, adapter_contract_version=4)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_candidate_does_not_change_valid_minute_template_values_or_metadata(engine):
    legacy = videoland_visible_article(BeautifulSoup(page(),engine), page_url=URL, adapter_contract_version=4)
    candidate = videoland_visible_article(BeautifulSoup(page(),engine), page_url=URL, adapter_contract_version=5)
    assert legacy[0] == candidate[0] and str(legacy[1]) == str(candidate[1])


@pytest.mark.parametrize('day', ['2026/6/29','2026/1/2','2026/12/31','2024/2/29'])
def test_date_is_exact_calendar_equality_not_fixed_width_or_datetime_imputation(day):
    year,month,date = map(int,day.split('/'))
    published = datetime(year,month,date,9,51,37).strftime('%Y-%m-%dT%H:%M:%S+08:00')
    modified = datetime(year,month,date,10,2,9).strftime('%Y-%m-%dT%H:%M:%S+08:00')
    soup = soup_for()
    soup.select_one('#NewsRealDate').string = day
    change_schema(soup,datePublished=published,dateModified=modified)
    for field,value in (('published_time',published),('modified_time',modified)):
        soup.select_one('meta[property="article:'+field+'"]')['content'] = value
    assert clocks(soup)[:2] == (published,modified)


@pytest.mark.parametrize('change', [
    lambda s:s.select_one('#NewsRealDate').__setattr__('string','2026/6/28'),
    lambda s:s.select_one('#NewsRealDate').__setattr__('string','2026/06/29'),
    lambda s:s.select_one('#NewsRealDate').__setattr__('string','2026/99/29'),
    lambda s:s.select_one('#NewsRealDate').__setattr__('string','2026/2/30'),
    lambda s:s.select_one('#NewsRealDate').__setattr__('string','2026/6/29 00:00'),
    lambda s:s.select_one('#NewsRealDate').__setattr__('string','發布2026/6/29'),
    lambda s:s.select_one('#NewsRealDate').__setitem__('hidden',''),
    lambda s:s.select_one('#NewsRealDate').decompose(),
    lambda s:s.select_one('meta[property="article:published_time"]').decompose(),
    lambda s:s.select_one('meta[property="article:modified_time"]').decompose(),
    lambda s:s.select_one('meta[property="article:published_time"]').__setitem__('content',REVISION),
    lambda s:change_schema(s,dateModified=RELEASE),
    lambda s:change_schema(s,datePublished='2026-06-29T09:51:37'),
    lambda s:s.select_one('header').append(BeautifulSoup('<span id="NewsRealDate">2026/6/29</span>','lxml')),
    lambda s:s.select_one('head').append(BeautifulSoup('<meta property="article:published_time" content="'+RELEASE+'">','lxml')),
    lambda s:s.select_one('#NewsRealDate').append(BeautifulSoup('<span hidden>private</span>','lxml')),
])
def test_missing_conflicting_hidden_or_unobserved_clocks_are_not_filled(change):
    with pytest.raises(NewsError):
        clocks(soup_for(change=change))


@pytest.mark.parametrize('field,value', [
    ('datePublished','2026-06-29T01:51:37Z'),
    ('dateModified','2026-06-29T02:02:09Z'),
    ('datePublished','2026-06-29T09:51:37+00:00'),
    ('dateModified','2026-06-29T09:00:00+08:00'),
])
def test_observed_template_does_not_infer_a_timezone_or_reorder_an_update(field,value):
    soup = soup_for()
    change_schema(soup,**{field:value})
    meta = 'article:published_time' if field == 'datePublished' else 'article:modified_time'
    soup.select_one('meta[property="'+meta+'"]').attrs['content'] = value
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        clocks(soup)


def test_visible_update_if_present_is_not_ignored_and_minute_template_stays_strict():
    soup = soup_for()
    soup.select_one('header').append(BeautifulSoup('<span id="NewsModDate">2026/06/29 10:02</span>','lxml'))
    assert 'visible_update_minute_matches' in clocks(soup)[2]
    soup.select_one('#NewsModDate').string = '2026/06/29 10:01'
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        clocks(soup)
    soup = soup_for()
    soup.select_one('#NewsRealDate').string = '2026/06/29 09:51'
    with pytest.raises(NewsError,match='invalid_bulk_publication_clock'):
        clocks(soup)


def test_plain_nonanchor_credit_is_not_promoted_even_when_candidate_clocks_agree():
    soup = soup_for()
    assert clocks(soup)[:2] == (RELEASE,REVISION)
    node = soup.select_one('#author')
    node.name = 'span'
    del node['href']
    node.string = '合成姓名'
    change_schema(soup,author={'@type':'Person','name':'合成姓名'})
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        videoland_visible_article(soup,page_url=URL,adapter_contract_version=5)


def test_engineering_candidate_is_not_a_registered_production_or_network_path(tmp_path):
    settings = versioned_config(5)
    with pytest.raises(ValueError,match='unreviewed embedded article parser'):
        PolicyRegistry(settings,base=tmp_path)
    settings = versioned_config(3)
    registry = PolicyRegistry(settings,base=tmp_path)
    with pytest.raises(ValueError,match='reviewed_videoland_v3_predecessor_candidate_required'):
        videoland_candidate_source(settings,registry,tmp_path,'videoland_visible_article_v5')
