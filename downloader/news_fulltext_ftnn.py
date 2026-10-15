"""FTNN native article proof; HTTP, scheduling and persistence stay shared."""
from copy import deepcopy
from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


FTNN_BODY_SELECTOR = ':scope > .news-body'
_AD_LABEL = '廣告 更多內容請繼續往下閱讀'


def _release_clocks(scope, declared):
    """Bind each native label to its own declared clock, never the first p.

    Revised originals display both 發布時間 and 異動時間. One of each is
    unambiguous; two publication labels, unknown text or mismatched minutes
    are not. Keep declared seconds and original labels separately, including
    single-digit source month/day formatting. Neither clock comes from the
    requested URL, related stories or capture time.
    """
    nodes = scope.select(':scope > .news-page-top > .news-detail-text > .ndt-left > p')
    if not 1 <= len(nodes) <= 2:
        raise NewsError('invalid_bulk_publication_clock')
    labels, native, notices = {}, {}, []
    try:
        for node in nodes:
            for descendant in (node, *node.find_all(True)):
                _visible(descendant)
            text = node.get_text(' ', strip=True)
            if text.startswith('發布時間：'):
                field, prefix, kind = 'datePublished', '發布時間：', 'native_visible_release_label'
            elif text.startswith('異動時間：'):
                field, prefix, kind = 'dateModified', '異動時間：', 'native_visible_modification_label'
            else:
                raise ValueError('unknown native clock label')
            if field in labels:
                raise ValueError('competing native clock labels')
            native[field] = datetime.strptime(text, prefix + '%Y/%m/%d %H:%M')
            labels[field] = text
            notices.append({'kind': kind, 'text': text})
        if 'datePublished' not in labels:
            raise ValueError('native publication label required')
        clocks = {}
        for field in ('datePublished', 'dateModified'):
            value = declared.get(field, '')
            if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00', str(value)):
                raise ValueError('unknown source timezone/precision')
            clocks[field] = datetime.fromisoformat(value)
            if field in native and clocks[field].replace(tzinfo=None, second=0) != native[field]:
                raise ValueError('competing source clock minutes')
        if clocks['dateModified'] < clocks['datePublished']:
            raise ValueError('source modification precedes publication')
    except (ValueError, TypeError):
        raise NewsError('invalid_bulk_publication_clock') from None
    return nodes, notices, len(labels) == 2


def ftnn_visible_article(soup, *, page_url):
    """Use the whole visible report, not description, ads or related stories.

    The source's NewsArticle has no URL/articleBody. Bind its single schema
    to canonical/OG/breadcrumb, the visible headline, reporter and release
    minute instead. A public reporter account is attribution evidence, not a
    grant of third-party rights or proof of historical point-in-time release.
    """
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.ftnn.com.tw' or parts.query or parts.fragment
            or not re.fullmatch(r'/news/[1-9][0-9]*', parts.path)):
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup, 'link[rel~="canonical"]')
    ogurl = _one(soup, 'meta[property="og:url"][content]')
    ogtitle = _one(soup, 'meta[property="og:title"][content]')
    scope = _one(soup, '.news-page-all > .news-show')
    heading = _one(scope, ':scope > .news-page-top > .news-main-title > h1')
    title = heading.get_text(' ', strip=True)
    compact = lambda value: re.sub(r'\s+', '', value) if isinstance(value, str) else None
    if (canonical.get('href') != page_url or ogurl.get('content') != page_url or not title
            or compact(ogtitle.get('content')) != compact(title + ' | FTNN 新聞網')):
        raise NewsError('article_identity_not_verified')
    declarations = []
    breadcrumbs = []
    try:
        for script in soup.select('script[type="application/ld+json"]'):
            for obj in _objects(json.loads(script.get_text())):
                if obj.get('@type') == 'NewsArticle':
                    declarations.append(obj)
                elif obj.get('@type') == 'BreadcrumbList':
                    breadcrumbs.append(obj)
    except (ValueError, TypeError):
        raise NewsError('article_identity_not_verified') from None
    if len(declarations) != 1 or len(breadcrumbs) != 1:
        raise NewsError('article_identity_not_verified')
    declared = declarations[0]
    crumbs = breadcrumbs[0].get('itemListElement')
    publisher = declared.get('publisher')
    if (not isinstance(crumbs, list) or not crumbs or not isinstance(crumbs[-1], dict)
            or crumbs[-1].get('item') != {'@id': page_url, 'name': title}
            or compact(declared.get('headline')) != compact(title)
            or not isinstance(publisher, dict) or publisher.get('name') != 'FTNN 新聞網'
            or any(declared.get(field, page_url) != page_url for field in ('url', '@id', 'mainEntityOfPage'))):
        raise NewsError('article_identity_not_verified')
    access = declared.get('isAccessibleForFree')
    if not (access is True or access == 'http://schema.org/True'):
        raise NewsError('paywall_or_login')
    # An unexpected full body cannot silently replace the reviewed visible one.
    if declared.get('articleBody'):
        raise NewsError('article_identity_not_verified')
    box = _one(scope, ':scope > .news-box')
    body = _one(box, FTNN_BODY_SELECTOR)
    byline = _one(body, ':scope > p:first-child', 'article_attribution_not_verified')
    author = declared.get('author')
    if (not isinstance(author, dict) or author.get('@type') != 'Person'
            or not isinstance(author.get('name'), str) or not author['name'].strip()
            or not re.fullmatch(r'https://www[.]ftnn[.]com[.]tw/search\?author=a[0-9]+', str(author.get('url', '')))):
        raise NewsError('article_attribution_not_verified')
    credit = byline.get_text(' ', strip=True)
    if not re.fullmatch(r'(?:FTNN新聞網)?記者' + re.escape(compact(author['name'])) + r'／[^／\s]{1,16}報導', compact(credit)):
        raise NewsError('article_rights_or_scope_excluded')
    clock_nodes, clock_notices, dual_clock = _release_clocks(scope, declared)
    for node in (scope, heading, box, body, byline, *clock_nodes):
        _visible(node)
    if body.select('script, article, form, [rel~="license"]'):
        raise NewsError('article_rights_or_scope_excluded')
    # Keep the original document unchanged. Only verified display plumbing and
    # the already-recorded byline are removed from this one small body copy.
    cleaned = deepcopy(box)
    clean_body = _one(cleaned, FTNN_BODY_SELECTOR)
    clean_body.select_one(':scope > p:first-child').decompose()
    for node in list(clean_body.select(':scope > p')):
        if node.get_text(' ', strip=True) == _AD_LABEL:
            following = node.find_next_sibling()
            if following is None or not re.fullmatch(r'div-gpt-ad-[0-9]+-[0-9]+', following.get('id', '')):
                raise NewsError('article_identity_not_verified')
            node.decompose()
    for node in list(clean_body.select('[id^="div-gpt-ad-"]')):
        if node.get_text(' ', strip=True):
            raise NewsError('article_identity_not_verified')
        node.decompose()
    captions = [node.get_text(' ', strip=True) for node in clean_body.select('figure.image figcaption')]
    for node in clean_body.find_all(True):
        if not node.find_parent('figure'):
            _visible(node)
    return {
        'headline': title, 'author': [author], 'datePublished': declared['datePublished'],
        'dateModified': declared['dateModified'],
        'author_attribution_detail_json': json.dumps({'native_byline': credit,
            'source_reporter_account': author['url'], 'author_name_invented': False}, ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'published_time_basis': ('source_declared_aware_datetimes_correspond_to_visible_release_and_modification_minutes; not_observed_historical_PIT'
            if dual_clock else 'source_declared_aware_datetime_corresponds_to_visible_release_minute; not_observed_historical_PIT'),
        'verified_article_notices_json': json.dumps(clock_notices, ensure_ascii=False),
        'article_identity_basis': ('canonical_OG_breadcrumb_single_schema_heading_reporter_and_visible_release_revision_minutes_v2'
            if dual_clock else 'canonical_OG_breadcrumb_single_schema_heading_reporter_and_visible_release_minute_v1'),
        'body_completeness': 'verified_current_whole_native_report; source_risk_notice_retained; advertising_and_related_stories_excluded; captions_recorded_separately',
    }, cleaned
