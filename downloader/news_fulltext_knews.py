"""Finite Knews public originals, with distinct header and printed credits.

Only the two reviewed literal originals are in scope. This pure native parser
does not fetch anything, infer that different names identify the same person,
or turn a public response into a publisher-wide permission or historical clock.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from bs4 import Tag

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


KNEWS_LAYOUT = 'knews_public_native_article_v1'
KNEWS_PATH_PATTERN = r'^/news/[0-9A-F]{32}$'
KNEWS_BODY_SELECTOR = (':scope > figure > figcaption, :scope > p.post-intro, '
                       ':scope > .article-body-wrapper > .article-content-inner')
KNEWS_AUTHOR_SELECTOR = ':scope > .article-meta-panel .abc-byline > a'
KNEWS_EXCLUSIONS = ('.in-article-ad-wrapper', '.related-news-card', '.extend-reading', '.app-promo-wrapper')
KNEWS_REVIEWED_HEADER_AUTHORS = ('芮內', '克里夫')
KNEWS_REVIEWED_URLS = (
    'https://www.knews.com.tw/news/04CE23AF3987F7A667AECC0810CDBF4D',
    'https://www.knews.com.tw/news/F84F5BD478EA9B742886C5819E33B9D7',
)
# Exact reviewed capture/auth -> original HTTP owner. An adapter/scope review
# does not create a new REP/refusal/rate budget for the same public origin.
KNEWS_FINITE_HTTP_BINDING = (
    '83978a41e8b8a02c82c353b66f44a36dd6af87e0acc781a090bc7e96fb740222',
    '7eb32b35ba2cb8fc29e68da4a7ce4cc3545e585e21dad33012931a746b9bf422',
    '4b64e6ca583ff8dadef050f6412bec1f773ed3166da81c489e52f2af154c3080',
    'f7f59993ddf223e07c708ccdb904e6f3e1248364426e102d1a23d9e6213a3beb',
)
_ORIGIN = 'https://www.knews.com.tw'
_PROSE = frozenset({'p', 'h3', 'br', 'strong', 'b', 'em', 'i', 'u', 's', 'a', 'span',
                    'ul', 'ol', 'li', 'blockquote', 'sub', 'sup', 'figure', 'figcaption', 'img'})


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    return value


def _article_link(value):
    parts = urlsplit(value or '')
    return (parts.scheme == 'https' and parts.netloc == 'www.knews.com.tw'
            and re.fullmatch(KNEWS_PATH_PATTERN, parts.path) and not parts.fragment)


def _children(node):
    for child in node.children:
        if isinstance(child, Tag):
            yield child
        elif str(child).strip():
            raise NewsError('article_identity_not_verified')


def _body_structure(scope, intro, body, figure):
    """Validate the complete witnessed editor, not an arbitrary selector slice."""
    wrapper = body.parent
    if list(_children(wrapper)) != [_one(wrapper, ':scope > #article-left-toc.article-left-toc'), body]:
        raise NewsError('article_identity_not_verified')
    toc = wrapper.select_one('#article-left-toc')
    _visible(toc)
    headings = body.select(':scope > h3')
    links = toc.select('.toc-list > li > a.toc-link')
    if (not headings or len(links) != len(headings)
            or any(h.get('id') != f'subhead-{index}' or link.get('href') != '#' + h['id']
                   or link.get_text(' ', strip=True) != h.get_text(' ', strip=True)
                   for index, (h, link) in enumerate(zip(headings, links)))):
        raise NewsError('article_identity_not_verified')
    excluded = set()
    for node in _children(body):
        classes = node.get('class', [])
        if classes == ['in-article-ad-wrapper']:
            if (node.name != 'div' or node.get_text(strip=True)
                    or any(child.name != 'div' for child in node.find_all())):
                raise NewsError('article_identity_not_verified')
        elif classes == ['related-news-card']:
            link = _one(node, ':scope > a')
            label, headline = _one(link, 'span'), _one(link, 'h5')
            if (node.name != 'div' or node.get('id') != 'norss'
                    or not _article_link(link.get('href'))
                    or list(_children(node)) != [link]
                    or label.get_text(strip=True) != '編輯推薦' or len(link.select('img')) != 1
                    or node.get_text(strip=True) != label.get_text(strip=True) + headline.get_text(strip=True)
                    or any(child.name not in {'a', 'div', 'img', 'span', 'h5'} for child in node.find_all())):
                raise NewsError('article_identity_not_verified')
        elif classes == ['extend-reading']:
            if node.name != 'p':
                raise NewsError('article_identity_not_verified')
            if node.get_text(strip=True):
                if (_one(node, ':scope > span').get_text(' ', strip=True) != '🔗 延伸閱讀'
                        or not _article_link(_one(node, ':scope > a').get('href'))
                        or [c.name for c in _children(node)] != ['span', 'a']):
                    raise NewsError('article_identity_not_verified')
            elif list(_children(node)):
                raise NewsError('article_identity_not_verified')
        elif classes == ['app-promo-wrapper']:
            link = _one(node, ':scope > a.app-promo-link')
            if (node.name != 'p' or link.get('href') != 'https://event.knews.com.tw/app'
                    or list(_children(node)) != [link]
                    or link.find() is not None
                    or link.get_text(' ', strip=True) != '⭐️ 即刻下載！無廣告《知新聞》App'):
                raise NewsError('article_identity_not_verified')
        else:
            if node.name not in {'p', 'h3'} or classes:
                raise NewsError('article_identity_not_verified')
            continue
        excluded.add(id(node))
    for container in (intro, body, figure):
        _visible(container)
        for node in container.find_all():
            if id(node) in excluded or any(id(parent) in excluded for parent in node.parents):
                continue
            if node.name not in _PROSE:
                raise NewsError('article_identity_not_verified')
            _visible(node)
    _visible(_one(figure, ':scope > figcaption'))
    # The shared text contract deliberately omits figures. These two editors
    # contain only empty promotional figures; do not silently lose a future
    # written inline caption by pretending it was covered by that contract.
    if any(node.get_text(strip=True) for node in body.select('figure')):
        raise NewsError('article_identity_not_verified')
    # A visible introduction/schema description alone is not the complete story.
    prose = [n for n in body.select(':scope > p') if id(n) not in excluded]
    if sum(len(n.get_text(' ', strip=True)) for n in prose) < 100:
        raise NewsError('body_not_found')
    expected = {'h1', 'h2', 'figure', 'p', 'div', 'script', 'style'}
    for node in _children(scope):
        if node.name not in expected:
            raise NewsError('article_identity_not_verified')
        if node.name == 'h2' and node.get_text(strip=True):
            raise NewsError('article_identity_not_verified')
        if node.name == 'p' and node is not intro:
            raise NewsError('article_identity_not_verified')
        if node.name == 'div' and node is not wrapper and not (
                node.get('id') == 'adm-wrapper' and node.get('class') == ['ad']
                or node.get('class') in (['knews-ticker-root'], ['article-meta-panel', 'abc-style'],
                                         ['post-share', 'sharetop'])):
            raise NewsError('article_identity_not_verified')


def knews_public_native_article(soup, *, page_url):
    if page_url not in KNEWS_REVIEWED_URLS:
        raise NewsError('article_identity_not_verified')
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    articles = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            articles.extend(obj for obj in _objects(json.loads(script.get_text()))
                            if obj.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared, scope = articles[0], _one(soup, 'article#post-detail')
    _visible(scope)
    if 'isAccessibleForFree' in declared and declared['isAccessibleForFree'] is not True:
        raise NewsError('paywall_or_login')
    if (declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or not isinstance(declared.get('publisher'), dict)
            or declared.get('publisher', {}).get('@type') != 'Organization'
            or declared['publisher'].get('name') != '知新聞'):
        raise NewsError('article_identity_not_verified')
    title = _one(scope, ':scope > h1')
    _visible(title)
    headline = title.get_text(' ', strip=True)
    if (not headline or headline != declared.get('headline')
            or _one(soup, 'head > meta[property="og:title"]').get('content') != headline + '｜知新聞'):
        raise NewsError('article_identity_not_verified')
    credit = _one(scope, KNEWS_AUTHOR_SELECTOR)
    _visible(credit)
    name, profile = credit.get_text(' ', strip=True), credit.get('href')
    if (_one(credit.parent, ':scope > span.label').get_text(strip=True) != '記者'
            or name not in KNEWS_REVIEWED_HEADER_AUTHORS
            or not isinstance(profile, str) or not re.fullmatch(re.escape(_ORIGIN) + r'/reporter/[a-z]+', profile)
            or _one(soup, 'head > meta[name=author]').get('content') != name
            or declared.get('author') != [{'@type': 'Person', 'name': name, 'url': profile}]):
        raise NewsError('article_attribution_not_verified')
    published, modified = _clock(declared.get('datePublished')), _clock(declared.get('dateModified'))
    timestamp = _one(scope, ':scope > .article-meta-panel .abc-timestamp')
    visible_clock = _one(timestamp, ':scope > time')
    _visible(visible_clock)
    visible_raw = visible_clock.get_text(' ', strip=True)
    if (_one(timestamp, ':scope > span.label').get_text(strip=True) != '發布'
            or visible_raw != datetime.fromisoformat(published).strftime('%Y/%m/%d %H:%M')
            or datetime.fromisoformat(modified) < datetime.fromisoformat(published)):
        raise NewsError('invalid_bulk_publication_clock')
    intro = _one(scope, ':scope > p.post-intro')
    body = _one(scope, ':scope > .article-body-wrapper > .article-content-inner')
    figure = _one(scope, ':scope > figure')
    printed = re.match(r'^【記者([^／\s【】]{1,30})／([^【】]{1,30})報導】', intro.get_text(' ', strip=True))
    if printed is None:
        raise NewsError('article_attribution_not_verified')
    _body_structure(scope, intro, body, figure)
    detail = {'contract_version': 1, 'source_header_credit': name, 'literal_profile_url': profile,
        'source_schema_author_name': name, 'source_schema_author_type_raw': 'Person',
        'printed_byline_raw': printed[0], 'printed_reporter_name': printed[1],
        'printed_reporting_location': printed[2],
        'header_and_printed_names_equal': name == printed[1],
        'different_names_are_verified_aliases': False, 'human_reporter_identity_verified': False,
        'original_ownership_verified': False,
        'source_credit_roles': ['publisher_current_header_credit', 'source_printed_reporter_signature']}
    notices = {'contract_version': 1, 'visible_publication_clock_raw': visible_raw,
        'visible_publication_precision': 'minute', 'schema_publication_clock_raw': published,
        'schema_modified_clock_raw': modified, 'schema_clock_precision': 'second',
        'schema_timezone_offset_raw': '+08:00', 'visible_timezone_declared': False,
        'historical_first_release_verified': False, 'schema_description_used_as_full_body': False,
        'media_or_scripts_requested_or_executed': False}
    return {'headline': headline, 'author': [{'name': name}], 'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'publisher_schema_declared_seconds_offset_matching_visible_minute; no_historical_first_release_proof',
        'article_identity_basis': 'literal_reviewed_URL_canonical_og_schema_headline_native_editor_and_distinct_credits_v1',
        'author_attribution_detail_json': json.dumps(detail, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': 'whole_visible_main_caption_intro_and_editor_prose_in_source_order_once; toc_related_app_ads_excluded; no_media_transcription'}, scope
