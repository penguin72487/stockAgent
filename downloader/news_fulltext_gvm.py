"""Reviewed GVM public native articles; no HTTP, hidden text or script execution.

This first adapter is finite and private. Publisher-declared reporter and
advertorial credits remain different roles, not verified people/ownership.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


GVM_LAYOUT = 'gvm_public_native_article_v1'
GVM_PATH_PATTERN = r'^/article/[1-9][0-9]*$'
GVM_BODY_SELECTOR = ':scope > .article-content.article-content_level-0'
GVM_AUTHOR_SELECTOR = ':scope > .article-head .article-head_author'
GVM_EXCLUSIONS = ('.newsletter', '.article-head_marketing', '#article_float_article',
                  '#ad_article_video', '[id^="div-gpt-ad-"]')
GVM_REVIEWED_AUTHORS = ('盧佳柔', '遠見雜誌整合傳播部企劃製作')
# Exact finite capture/auth -> the actual earlier two-original HTTP owner.
# A different URL set, policy, endpoint or pacing must not inherit calibration.
GVM_FINITE_HTTP_BINDING = (
    '5cd98e438146ec9b72d8e02b726492e977baa8266adb6b18c4d6f2dffd3fcae9',
    '27372f5fea2a6c3e76d716b0b225f8f85ce1944100dc961332b86d166a271ace',
    'd053e657612ea17db48c41971030b3554e51357bbd33b1674a7f4dca1a877a7f',
    'eddf93f47e81758f1908611db46dd82274f0542d432967665bd1840376e6661a',
)
_ORIGIN = 'https://www.gvm.com.tw'
_AD = re.compile(r'div-gpt-ad-[0-9]{13}-[0-9]+-([1-9][0-9]*)')
_AD_SCRIPT = re.compile(r"\s*googletag[.]cmd[.]push\(function \(\) \{ googletag[.]display\('([^']+)'\); \}\);\s*")
_PROSE = frozenset({'p', 'blockquote', 'h2', 'h3', 'h4', 'ul', 'ol', 'li', 'br',
    'strong', 'b', 'em', 'i', 'u', 's', 'a', 'span', 'img', 'figcaption', 'sub', 'sup'})


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    return value  # Retain declared precision/offset; never start_time or fetch time.


def _body_structure(body, article_id):
    """Check the witnessed static editor tree before shared text extraction.

    Newsletter, marketing and empty ad mounts are independently bounded direct
    children. No page-wide fallback, executing a loader or fetching a podcast.
    """
    excluded = set()
    for selector in GVM_EXCLUSIONS:
        for node in body.select(selector):
            if node.parent is not body:
                raise NewsError('article_identity_not_verified')
            identifier = node.get('id', '')
            if identifier.startswith('div-gpt-ad-'):
                if (not _AD.fullmatch(identifier) or not identifier.endswith('-' + article_id)
                        or node.get_text(strip=True)):
                    raise NewsError('article_identity_not_verified')
            elif identifier in {'article_float_article', 'ad_article_video'} and node.get_text(strip=True):
                raise NewsError('article_identity_not_verified')
            excluded.add(id(node))
    for script in body.find_all('script'):
        match = _AD_SCRIPT.fullmatch(script.get_text()) if not script.attrs else None
        slot = script.parent
        if (match is None or not _AD.fullmatch(match[1]) or not match[1].endswith('-' + article_id)
                or slot.parent is not body or slot.name != 'div' or slot.get('id') != match[1]
                or id(slot) not in excluded
                or any(text.strip() for text in slot.find_all(string=True) if text.parent is not script)):
            raise NewsError('article_identity_not_verified')
    for node in body.find_all():
        if id(node) in excluded or any(id(parent) in excluded for parent in node.parents):
            continue
        if node.name == 'input' and node.parent is body:
            if (node.get('type') == 'hidden' and node.get('id') == 'tracker[]'
                    and node.get('key') == 'banners' and str(node.get('value', '')).isdigit()):
                continue  # Native ad telemetry, not an observation or written text.
        if node.name == 'iframe':
            parts = urlsplit(node.get('src', ''))
            if (node.parent is body and node.get('title') == 'Firstory'
                    and parts.scheme == 'https' and parts.netloc == 'open.firstory.me'
                    and re.fullmatch(r'/embed/story/[a-z0-9]+', parts.path)
                    and not parts.query and not parts.fragment and not node.get_text(strip=True)):
                _visible(node)
                continue  # Unrequested external audio is not captured prose.
        if node.name not in _PROSE:
            raise NewsError('article_identity_not_verified')
        _visible(node)
    lead = _one(body, ':scope > blockquote.article-head_blockquote')
    _visible(lead)
    paragraphs = body.find_all('p', recursive=False)
    if not paragraphs or sum(len(n.get_text(' ', strip=True)) for n in paragraphs) < 100:
        raise NewsError('body_not_found')  # A lead/summary alone is never complete.


def gvm_public_native_article(soup, *, page_url):
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.gvm.com.tw'
            or parts.query or parts.fragment or not re.fullmatch(GVM_PATH_PATTERN, parts.path)):
        raise NewsError('article_identity_not_verified')
    article_id = parts.path.rsplit('/', 1)[1]
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    head = _one(soup, '.article-left > .article-block > .article-block > .article-head')
    scope = head.parent
    body = _one(scope, GVM_BODY_SELECTOR)
    _visible(head)
    _visible(body)
    marker = _one(scope.parent, ':scope > input#article_id')
    if (marker.get('type') != 'hidden' or marker.get('value') != article_id
            or head.get('aid') != article_id or head.get('id') != 'article_' + article_id):
        raise NewsError('article_identity_not_verified')
    if (head.get('data-is-accessible') != '1' or head.get('status') != '不限制'
            or body.get('class') != ['article-content', 'article-content_level-0']):
        raise NewsError('paywall_or_login')
    articles = []
    for script in soup.select('head script[type="application/ld+json"]'):
        try:
            articles.extend(value for value in _objects(json.loads(script.get_text()))
                            if value.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared = articles[0]
    if declared.get('isAccessibleForFree') is not True:
        raise NewsError('paywall_or_login')
    if (declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or declared.get('publisher') != {'@id': _ORIGIN + '/#publisher'}):
        raise NewsError('article_identity_not_verified')
    title = _one(head, ':scope > h1.article-head_h1')
    _visible(title)
    headline = title.get_text(' ', strip=True)
    if (not headline or headline != head.get('title') or headline != declared.get('headline')
            or _one(soup, 'head > meta[property="og:title"]').get('content') != headline + ' | 遠見雜誌'):
        raise NewsError('article_identity_not_verified')
    credit = _one(head, '.article-head_inner > a.article-head_author')
    _visible(credit)
    name, profile = credit.get_text(' ', strip=True), credit.get('href')
    role = {'網路文章': 'publisher_declared_reporter', '廣編': 'publisher_declared_advertorial'}.get(head.get('article_type'))
    route = 'blog' if role == 'publisher_declared_advertorial' else 'author'
    author_id = head.get('author_id', '')
    if (role is None or name not in GVM_REVIEWED_AUTHORS or head.get('author') != name
            or not re.fullmatch(r'[1-9][0-9]*', author_id) or profile != f'{_ORIGIN}/{route}/{author_id}'
            or declared.get('author') != [{'@type': 'Person', 'name': name, 'url': profile}]
            or (role == 'publisher_declared_advertorial') != (name == GVM_REVIEWED_AUTHORS[1])):
        raise NewsError('article_rights_or_scope_excluded')
    published = _clock(declared.get('datePublished'))
    modified_schema = _clock(declared.get('dateModified'))
    modified_meta = _clock(_one(soup, 'head > meta[property="article:modified_time"]').get('content'))
    visible_date = _one(head, '.article-head_box > p.article-head_grey:not(.js-article-click)')
    _visible(visible_date)
    if (published != _one(soup, 'head > meta[property="article:published_time"]').get('content')
            or head.get('pub_date') != published[:10] or visible_date.get_text(strip=True) != published[:10]
            or datetime.fromisoformat(modified_meta) < datetime.fromisoformat(published)
            or datetime.fromisoformat(modified_schema) < datetime.fromisoformat(published)):
        raise NewsError('invalid_bulk_publication_clock')
    _body_structure(body, article_id)
    detail = {'contract_version': 1, 'source_visible_credit': name, 'literal_profile_url': profile,
        'publisher_declared_article_type': head['article_type'], 'source_credit_role': role,
        'schema_author_type_raw': 'Person', 'human_reporter_identity_verified': False,
        'original_ownership_verified': False, 'sponsored_content_is_independent_editorial': False}
    notices = {'contract_version': 1, 'source_publication_meta_raw': published,
        'source_schema_modified_raw': modified_schema, 'source_og_modified_raw': modified_meta,
        'modified_declarations_agree': modified_schema == modified_meta,
        'visible_publication_date_raw': visible_date.get_text(strip=True),
        'native_start_time_is_publication': False,
        'media_or_scripts_requested_or_executed': False, 'historical_first_release_verified': False}
    return {'headline': headline, 'author': [{'name': name}], 'datePublished': published,
        'dateModified': modified_meta,
        'published_time_basis': 'publisher_declared_metadata_seconds_and_offset; visible_date_only; no_historical_first_release_proof',
        'article_identity_basis': 'literal_canonical_og_URL_native_ID_header_schema_credit_public_level0_v1',
        'author_attribution_detail_json': json.dumps(detail, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': 'whole_public_native_editor_written_text_with_visible_captions; summary_not_promoted; newsletter_ads_excluded; audio_images_not_transcribed'}, scope
