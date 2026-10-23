"""Finite, public CW editor compilations; metadata is not hidden body text.

The reviewed native page splits one public article into two static containers.
Both are required; a preview, third-party credit or schema articleBody is never
a full-body substitute. No JavaScript, API, media or account requests occur.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


CW_LAYOUT = 'cw_public_editor_compilation_v1'
CW_ORIGIN = 'https://www.cw.com.tw'
CW_PATH_PATTERN = r'^/article/[1-9][0-9]*$'
CW_BODY_SELECTOR = ':scope > .article__body > .article__content'
CW_AUTHOR_SELECTOR = ':scope > .article__body > .article__detail > .article__author .author--item > a'
CW_EDITOR = '天下編輯部'
CW_EXCLUSIONS = ('.cw__advertising',)
# Only this narrowed one-original admission inherits its actual earlier
# two-original diagnostic HTTP identity. Never create a fresh refusal budget
# for a parser/display change or extend this mapping to another URL or host.
CW_FINITE_HTTP_BINDING = (
    '2d3f1cbbd00ce24ba36720eab5f36d0c5ec43ae4b971900f9e038068a144f0f7',
    '43d8a14dfb74f72e19fe53154411b6ea9351923c93069cb42d2699f61a6e0509',
    'e5eb5c9c730e9924ae7a7325032fdc85827056f99d3b71855834eedf43a8e9a2',
    '36630d45739f18c6af735d604102cc62b2108fc5df79c239ecd04a8b2f743843',
)
_WRITTEN_TAGS = frozenset({'div', 'p', 'h2', 'h3', 'h4', 'a', 'br', 'span',
    'strong', 'b', 'em', 'i', 'u', 's', 'sub', 'sup', 'ul', 'ol', 'li',
    'table', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'caption'})


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    return value


def _written_body(body):
    preview = _one(body, ':scope > #article-preview.article-preview')
    full = _one(body, ':scope > #article-full-content.article-content')
    if body.find_all(recursive=False) != [preview, full]:
        raise NewsError('article_identity_not_verified')
    if (not preview.get_text(strip=True) or len(full.get_text(' ', strip=True)) < 100
            or not full.find('p', recursive=False)):
        raise NewsError('body_not_found')  # A free preview alone proves no whole article.
    # The root's ancestors were checked once. Walk each descendant once and
    # check only its own visibility; no repeated ancestor walks or DOM copies.
    stack = [body]
    while stack:
        node = stack.pop()
        style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
        if (node.has_attr('hidden') or str(node.get('aria-hidden', '')).lower() == 'true'
                or 'display:none' in style or 'visibility:hidden' in style):
            raise NewsError('article_identity_not_verified')
        if 'cw__advertising' in node.get('class', []):
            # Only witnessed static ad shells may be excluded. Never silently
            # discard prose or scripts merely because they carry an ad class.
            if (node.name != 'section' or node.parent not in [preview, full]
                    or node.get_text(' ', strip=True) != '廣告'
                    or [n.name for n in node.find_all(recursive=False)] != ['span', 'div']):
                raise NewsError('article_identity_not_verified')
            label, mount = node.find_all(recursive=False)
            if (label.get_text(strip=True) != '廣告' or label.find_all()
                    or mount.get('class') != ['google-ads'] or mount.contents
                    or not re.fullmatch(r'div-gpt-ad-[0-9]{13}-[0-9]+', mount.get('id', ''))):
                raise NewsError('article_identity_not_verified')
            continue
        if node.name not in _WRITTEN_TAGS:
            raise NewsError('article_identity_not_verified')
        stack.extend(reversed(node.find_all(recursive=False)))
    return preview, full


def cw_public_editor_compilation(soup, *, page_url):
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.cw.com.tw'
            or parts.query or parts.fragment or not re.fullmatch(CW_PATH_PATTERN, parts.path)):
        raise NewsError('article_identity_not_verified')
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    articles = []
    for script in soup.select('head script[type="application/ld+json"]'):
        try:
            articles.extend(obj for obj in _objects(json.loads(script.get_text()))
                            if obj.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared = articles[0]
    if declared.get('isAccessibleForFree') is not True:
        raise NewsError('paywall_or_login')
    if (declared.get('@id') != page_url+'#article' or declared.get('url') != page_url
            or declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or declared.get('publisher') != {'@id': CW_ORIGIN+'/#organization'}):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'main > article')
    _visible(scope)
    title = _one(scope, ':scope > .article__head h1')
    _visible(title)
    headline = title.get_text(' ', strip=True)
    if (not headline or headline != declared.get('headline')
            or _one(soup, 'head > meta[property="og:title"]').get('content') != headline+'｜天下雜誌'):
        raise NewsError('article_identity_not_verified')
    detail = _one(scope, ':scope > .article__body > .article__detail')
    _visible(detail)
    credit = _one(detail, ':scope > ul.article__author')
    profile = CW_ORIGIN+'/author/30'
    expected_author = {'@type': 'Person', 'name': CW_EDITOR, '@id': profile+'#person', 'url': profile,
        'worksFor': {'@type': 'Organization', '@id': CW_ORIGIN+'/#organization'}}
    bylines = credit.select(':scope > li')
    if (len(bylines) != 1 or declared.get('author') != [expected_author]
            or _one(bylines[0], ':scope > span').get_text(strip=True) != '整理'):
        raise NewsError('article_rights_or_scope_excluded')
    link = _one(bylines[0], ':scope > .author--item > a')
    _visible(link)
    if (len(credit.select('a')) != 1 or link.get('href') != profile
            or link.get_text(strip=True) != CW_EDITOR or credit.get_text(' ', strip=True) != '整理 '+CW_EDITOR):
        raise NewsError('article_rights_or_scope_excluded')
    published = _clock(declared.get('datePublished'))
    modified = _clock(declared.get('dateModified'))
    meta_modified = _clock(_one(soup, 'head > meta[property="article:modified_time"]').get('content'))
    visible_clock = _one(detail, ':scope > .pt10 > time')
    _visible(visible_clock)
    visible_date = visible_clock.get_text(strip=True)
    if (modified != meta_modified or datetime.fromisoformat(modified) < datetime.fromisoformat(published)
            or visible_date not in {'更新時間：'+modified[:10], '發布時間：'+published[:10]}):
        raise NewsError('invalid_bulk_publication_clock')
    body = _one(scope, CW_BODY_SELECTOR)
    _visible(body)
    preview, full = _written_body(body)
    attribution = {'contract_version': 1, 'source_visible_credit': CW_EDITOR,
        'source_credit_role': 'publisher_declared_editor_compilation',
        'schema_author_type_raw': 'Person', 'literal_profile_url': profile,
        'human_reporter_identity_verified': False, 'original_ownership_verified': False,
        'third_party_source_credit_admitted': False}
    notices = {'contract_version': 1, 'visible_current_clock_raw': visible_date,
        'source_schema_publication_raw': published, 'source_schema_modified_raw': modified,
        'source_OG_modified_raw': meta_modified, 'source_declares_free_access': True,
        'public_static_preview_and_full_both_required': True,
        'preview_characters_before_exclusions': len(preview.get_text(' ', strip=True)),
        'full_characters_before_exclusions': len(full.get_text(' ', strip=True)),
        'expandable_static_FAQ_is_not_a_paid_hidden_body': True,
        'media_or_scripts_requested_or_executed': False,
        'historical_first_release_verified': False, 'old_metadata_used_as_publication': False}
    return {'headline': headline, 'author': [{'name': CW_EDITOR}],
        'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'current_publisher_schema_seconds_and_offset; visible_date_only; no_historical_first_release_proof',
        'article_identity_basis': 'literal_canonical_OG_URL_schema_ID_and_visible_editor_compilation_v1',
        'attribution_status': 'source_declared_corporate_author_not_verified_person',
        'author_attribution_detail_json': json.dumps(attribution, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': 'whole_static_public_preview_plus_full_written_body_tables_and_expandable_FAQ; ads_excluded; no_summary_or_schema_body_fallback'}, scope
