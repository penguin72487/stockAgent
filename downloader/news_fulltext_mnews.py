"""Pure Mirror TV native proof; transport, budgets and durable storage stay shared.

The visible lead and continuation are separate article elements. Neither a
schema description nor a video-only lead proves a complete written report.
Foreign /external and mm- stories are not original Mirror TV text by URL.
"""
from copy import deepcopy
from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


MNEWS_PATH_PATTERN = r'^/story/[0-9]{8}(?:nm|sot)[0-9]+$'
MNEWS_BODY_SELECTOR = (':scope > [class^="article-brief_briefWrapper__"] > article, '
                       ':scope > [class^="story_contentWrapper__"] > article')
_SCOPE = 'main[class^="story_article__"] > section[class^="story_article__"]'
_BRIEF = ':scope > [class^="article-brief_briefWrapper__"] > article'
_REPORT = ':scope > [class^="story_contentWrapper__"] > article'
_AD = ':scope > div[class^="gpt-ad_reserver__"]'
_EMBED = ':scope > section[class^="embed-code-block_container__"]'


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('competing source JSON keys')
        result[key] = value
    return result


def _declarations(soup):
    scripts = soup.select('script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 16:
        raise NewsError('article_identity_not_verified')
    objects = []
    try:
        for node in scripts:
            text = node.get_text()
            if len(text) > 128 << 10:
                raise ValueError('unbounded source declaration')
            objects.extend(_objects(json.loads(text, object_pairs_hook=_unique_object)))
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    articles = [obj for obj in objects if obj.get('@type') == 'NewsArticle']
    crumbs = [obj for obj in objects if obj.get('@type') == 'BreadcrumbList']
    if len(articles) != 1 or len(crumbs) != 1:
        raise NewsError('article_identity_not_verified')
    return articles[0], crumbs[0], objects


def _clocks(soup, scope, declared):
    selectors = (
        ('datePublished', 'article:published_time', ':scope > [class^="article-info_categoryAndPublishTime__"] > time.amp-pub-cat-publishTime'),
        ('dateModified', 'article:modified_time', ':scope > [class^="story_contentWrapper__"] > [class^="article-update-time_updateTime__"] > time'),
    )
    clocks, notices = {}, []
    for field, property_name, selector in selectors:
        node = _one(scope, selector, 'invalid_bulk_publication_clock')
        meta = _one(soup, f'meta[property="{property_name}"][content]', 'invalid_bulk_publication_clock')
        raw = declared.get(field)
        try:
            if (not isinstance(raw, str) or not re.fullmatch(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00', raw)
                    or node.get('datetime') != raw or meta.get('content') != raw):
                raise ValueError('independent source clocks disagree')
            clocks[field] = datetime.fromisoformat(raw)
            if node.get_text(' ', strip=True) != clocks[field].strftime('%Y.%m.%d %H:%M 臺北時間'):
                raise ValueError('visible source minute disagrees')
        except (ValueError, TypeError):
            raise NewsError('invalid_bulk_publication_clock') from None
        _visible(node)
        notices.append({'kind': 'native_visible_' + field, 'text': node.get_text(' ', strip=True)})
    if clocks['dateModified'] < clocks['datePublished']:
        raise NewsError('invalid_bulk_publication_clock')
    return notices


def mnews_visible_article(soup, *, page_url):
    """Verify the literal current page and both complete native prose regions.

    Source seconds/offsets, visible minute precision and raw reporter credits
    stay distinct. No article IDs, authors, missing prose or PIT are inferred.
    """
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.mnews.tw' or parts.query or parts.fragment
            or not re.fullmatch(MNEWS_PATH_PATTERN, parts.path)):
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup, 'link[rel~="canonical"]')
    ogurl = _one(soup, 'meta[property="og:url"][content]')
    ogtitle = _one(soup, 'meta[property="og:title"][content]')
    scope = _one(soup, _SCOPE)
    heading = _one(scope, ':scope > h1[class^="article-info_title__"]')
    title = heading.get_text(' ', strip=True)
    declared, breadcrumb, objects = _declarations(soup)
    crumbs = breadcrumb.get('itemListElement')
    publisher = declared.get('publisher')
    if (not title or canonical.get('href') != page_url or ogurl.get('content') != page_url
            or ogtitle.get('content') != title or declared.get('headline') != title
            or declared.get('url') != page_url
            or declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
            or publisher.get('name') != '鏡新聞'
            or not isinstance(crumbs, list) or not crumbs or not isinstance(crumbs[-1], dict)
            or crumbs[-1].get('item') != page_url or crumbs[-1].get('name') != title
            or declared.get('articleBody')):
        raise NewsError('article_identity_not_verified')
    if 'isAccessibleForFree' in declared and declared['isAccessibleForFree'] is not True:
        raise NewsError('paywall_or_login')
    notices = _clocks(soup, scope, declared)
    credits = _one(scope, ':scope > ul[class^="article-info_credit__"]', 'article_attribution_not_verified')
    names, native_credits = [], []
    for node in credits.select(':scope > li'):
        _visible(node)
        credit = node.get_text('', strip=True)
        if not credit:
            continue  # The observed second slot is blank, not an author.
        match = re.fullmatch(r'記者｜([^｜/／\r\n]{1,80})', credit)
        if not match:
            raise NewsError('article_rights_or_scope_excluded')
        names.append(match[1])
        native_credits.append(node.get_text(' ', strip=True))
    author = declared.get('author')
    authors = author if isinstance(author, list) else [author]
    metas = soup.select('meta[property="article:author"][content]')
    people = [obj for obj in objects if obj.get('@type') == 'Person']
    if (not 1 <= len(names) <= 8 or len(set(names)) != len(names) or len(authors) != len(names)
            or any(not isinstance(a, dict) or a.get('@type') != 'Person' for a in authors)
            or [a.get('name') for a in authors] != names
            or [node.get('content') for node in metas] != names
            or len(people) != len(names) or [p.get('name') for p in people] != names
            or any(not isinstance(p.get('brand'), dict) or p['brand'].get('name') != '鏡新聞'
                   or p['brand'].get('url') != 'https://www.mnews.tw' for p in people)):
        raise NewsError('article_attribution_not_verified')
    brief, report = _one(scope, _BRIEF), _one(scope, _REPORT)
    if len(scope.select('article')) != 2 or any(not any(
            c.startswith('api-data-renderer_apiDataArticle__') for c in n.get('class', [])) for n in (brief, report)):
        raise NewsError('article_identity_not_verified')
    for node in (scope, heading, credits, brief, report):
        _visible(node)
    cleaned = deepcopy(scope)
    clean_brief, clean_report = _one(cleaned, _BRIEF), _one(cleaned, _REPORT)
    caption_nodes = scope.select('figure figcaption')
    for node in caption_nodes:
        _visible(node)
    captions = [node.get_text(' ', strip=True) for node in caption_nodes]
    for body in (clean_brief, clean_report):
        if body.select('script, form, article, [rel~="license"]'):
            raise NewsError('article_rights_or_scope_excluded')
        for node in body.select(_AD + ', ' + _EMBED):
            if node.get_text(' ', strip=True):
                raise NewsError('article_identity_not_verified')
            node.decompose()  # Verified empty display plumbing, not prose.
        for figure in body.select('figure'):
            figure.decompose()  # Media/captions are not report paragraphs.
        for node in body.find_all(True):
            _visible(node)
    if not clean_report.get_text(' ', strip=True):
        videos = clean_report.select(':scope > div[class^="youtube-block_youtubeContainer__"] > iframe')
        if len(videos) == 1:
            video = urlsplit(videos[0].get('src', ''))
            if (video.scheme == 'https' and video.netloc == 'www.youtube.com'
                    and re.fullmatch(r'/embed/[A-Za-z0-9_-]{11}', video.path)
                    and len(clean_report.select('iframe')) == 1
                    and len(clean_report.find_all(recursive=False)) == 1):
                raise NewsError('source_body_embedded_only')
        raise NewsError('body_not_found')
    if not clean_brief.get_text(' ', strip=True):
        raise NewsError('body_not_found')
    if '鏡新聞' in names:
        raise NewsError('article_attribution_not_verified')  # Never invent a reporter from the publisher label.
    return {'headline': title, 'author': authors, 'datePublished': declared['datePublished'],
        'dateModified': declared['dateModified'],
        'author_attribution_detail_json': json.dumps({'native_credits': native_credits,
            'basis': 'visible_reporter_signature_matches_current_article_meta_schema_and_person_brand',
            'publisher_used_as_named_reporter': False}, ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': 'source_declared_aware_datetime_matches_visible_release_minute; not_observed_historical_PIT',
        'article_identity_basis': 'literal_canonical_OG_breadcrumb_single_schema_native_title_reporter_brand_and_release_revision_v1',
        'body_completeness': 'verified_current_whole_native_report_lead_and_continuation; captions_recorded_separately; video_lead_not_fulltext',
    }, cleaned
