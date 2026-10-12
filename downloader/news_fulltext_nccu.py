"""Reviewed NCCU campus-news text, using the canonical collector and writer.

The actual page has no canonical/og URL or NewsArticle schema. Its native CMS
article identifiers, page title, article heading and current release panel are
the identity proof. Do not invent missing metadata or follow media/social APIs.
"""
from __future__ import annotations

from datetime import datetime
import json
import re

from bs4 import NavigableString

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


NCCU_REVIEWED_URL = 'https://www.nccu.edu.tw/p/406-1000-9212,r17.php?Lang=zh-tw'
NCCU_BODY_SELECTOR = ':scope > .module-inner > section.mb > .mcont > .mpgdetail > .meditor'
NCCU_REVIEWED_CREDIT = '【校訊記者林亭報導】'


def nccu_visible_news(soup, *, page_url):
    """Only the previously observed campus reporter's exact main-host article.

    The galleries' native article IDs corroborate identity, but their SSU
    photographs/captions are separately credited and never article text.
    HTTP Last-Modified is not this article's revision or publication time.
    """
    if page_url != NCCU_REVIEWED_URL:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, '.module-detail')
    heading = _one(scope, ':scope > .module-inner > section.mb > .mpgtitle > h2.hdline')
    title = heading.get_text(' ', strip=True)
    page_titles = soup.select('head > title')
    if (not title or len(page_titles) != 1
            or re.sub(r'\s+', '', page_titles[0].get_text()) != re.sub(r'\s+', '', title + ' - 國立政治大學')):
        raise NewsError('article_identity_not_verified')
    media_ids = [n.get('id') for n in scope.select('[id^="PtDetail_"]')]
    if media_ids != ['PtDetail_9212_ImageList1', 'PtDetail_9212_ImageList2']:
        raise NewsError('article_identity_not_verified')
    counters = soup.select('.module-misc .artctrl > .PtStatistic > em > i[id$="_PtStatistic_9212"]')
    if len(counters) != 1 or not re.fullmatch(r'a[0-9a-f]{32}_PtStatistic_9212', counters[0].get('id', '')):
        raise NewsError('article_identity_not_verified')
    # Unexpected declarations are not a fallback body. This native contract
    # was observed without them; a new representation needs its own review.
    if soup.select('link[rel~="canonical"], meta[property="og:url"], script[type="application/ld+json"]'):
        raise NewsError('article_identity_not_verified')
    detail = _one(scope, ':scope > .module-inner > section.mb > .mcont > .mpgdetail')
    panel = _one(detail, ':scope > .ptinfoproperty', 'article_attribution_not_verified')
    dates = panel.select(':scope > .ptinfoproperty_date')
    units = panel.select(':scope > .ptinfoproperty_group')
    if len(dates) != 1 or len(units) != 1 or len(panel.find_all(recursive=False)) != 2:
        raise NewsError('article_attribution_not_verified')
    fields = []
    for node, expected_label in ((dates[0], '日期 :'), (units[0], '單位 :')):
        values = node.find_all('span', recursive=False)
        labels = ' '.join(str(n).strip() for n in node.contents if type(n) is NavigableString).strip()
        if len(values) != 1 or labels != expected_label or len(node.find_all(recursive=False)) != 1:
            raise NewsError('article_attribution_not_verified')
        fields.append(values[0].get_text(' ', strip=True))
    clock, unit = fields
    if unit != '秘書處':
        raise NewsError('article_rights_or_scope_excluded')
    try:
        if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', clock):
            raise ValueError('unreviewed publication precision')
        datetime.strptime(clock, '%Y-%m-%d')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    body = _one(scope, NCCU_BODY_SELECTOR)
    prose = [str(n).strip() for n in body.contents if type(n) is NavigableString and str(n).strip()]
    if not prose or prose[0] != NCCU_REVIEWED_CREDIT:
        raise NewsError('article_rights_or_scope_excluded')
    if len(prose) < 2:
        raise NewsError('body_not_found')
    # Observed original prose is direct text separated by <br>, not a hidden
    # schema, a teaser or another editor block. Unknown descendants fail closed.
    if any(n.name != 'br' for n in body.find_all(True)):
        raise NewsError('article_identity_not_verified')
    for node in (scope, heading, detail, panel, body, counters[0]):
        _visible(node)
    for node in panel.find_all(True):
        _visible(node)
    return {
        'headline': title,
        'author': [{'name': '林亭'}],
        'author_attribution_detail_json': json.dumps({
            'native_byline': NCCU_REVIEWED_CREDIT, 'role': '校訊記者', 'release_unit': unit,
        }, ensure_ascii=False),
        'datePublished': clock,
        'published_time_basis': 'publisher_declared_date_only; no_intraday_time_inferred',
        'article_identity_basis': 'exact_reviewed_main_host_url_native_CMS9212_two_gallery_ids_statistic_id_and_matching_title_heading_v1',
        'body_completeness': 'verified_current_campus_news_own_text_and_native_byline; separately_credited_media_and_captions_excluded',
    }, scope
