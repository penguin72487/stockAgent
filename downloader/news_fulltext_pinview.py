"""Pinview's observed public written article, without HTTP or discovery.

Only reviewed reporter signatures are admitted. A partner label, matching
headline, a guessed www alias or a page clock is not an originality witness.
The visible clock has seconds but no timezone or revision label: retain that
limitation instead of manufacturing an offset or historical PIT observation.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


PINVIEW_ORIGIN = 'https://www.pinview.com.tw'
PINVIEW_PATH_PATTERN = r'^/News/[1-9][0-9]*[.]html$'
PINVIEW_BODY_SELECTOR = ':scope > .pinview-native-body'
_CREDIT = '文／資深記者 李錦奇'
_V2_CREDIT = '文／資深記者 林憲祥'
_COPYRIGHT = '品觀點 版權所有 ©2022 品觀點 All Rights Reserved.'
_CLOCK = re.compile(r'[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}')
_BODY_TAGS = frozenset({'p', 'br', 'img', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'span',
    'a', 'strong', 'b', 'em', 'i', 'u', 's', 'del', 'sub', 'sup', 'ul', 'ol', 'li',
    'blockquote', 'pre', 'code', 'hr', 'table', 'caption', 'colgroup', 'col', 'thead',
    'tbody', 'tfoot', 'tr', 'th', 'td'})


def pinview_article_id(url, *, requested=False):
    """Both request/response IDs stay literal; no alternative host or route."""
    if not isinstance(url, str) or type(requested) is not bool:
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.pinview.com.tw'
            or parts.query or parts.fragment):
        return None
    match = re.fullmatch(r'/News/([1-9][0-9]*)[.]html', parts.path)
    return match[1] if match else None


def pinview_validation_metadata(soup, *, page_url):
    """Bounded declarations from the received DOM, never prose or a grant.

    A previously unreviewed credit must be observable without saving its
    article, clearing retry budgets or requesting the same document again.
    This helper neither mutates the DOM nor invokes the accepting adapter.
    """
    def text(value):
        if not isinstance(value, str):
            return {'type': type(value).__name__}
        return {'value': value[:512], 'characters': len(value),
                'sha256': hashlib.sha256(value.encode()).hexdigest(), 'truncated': len(value) > 512}

    def public_url(value):
        if not isinstance(value, str):
            return {'type': type(value).__name__}
        try:
            parts = urlsplit(value)
            display = (parts._replace(query='', fragment='').geturl()[:512]
                       if parts.scheme == 'https' and parts.netloc == 'www.pinview.com.tw'
                       and not parts.username and not parts.password else None)
        except ValueError:
            display = None
        return {'display_url': display, 'sha256': hashlib.sha256(value.encode()).hexdigest()}

    scopes = soup.select('.web-layout > .main-container > .article')
    headers = [node for scope in scopes[:8] for node in scope.select(':scope > .article-title')[:8]]
    headings = [node for header in headers for node in header.select(':scope > h1')[:8]]
    native = [node for scope in scopes[:8] for node in scope.select(':scope > div#contentText')[:8]]
    result = {
        'contract_version': 1, 'current_native_url_recognized': pinview_article_id(page_url) is not None,
        'article_scope_count': len(scopes), 'scope_sample_truncated': len(scopes) > 8,
        'sampled_header_count': len(headers), 'global_h1_count': len(soup.select('h1')),
        'headings': [text(node.get_text(' ', strip=True)) for node in headings[:8]],
        'head_titles': [text(node.get_text(' ', strip=True)) for node in soup.select('head > title')[:8]],
        'og_titles': [text(node.get('content')) for node in soup.select('head > meta[property="og:title"]')[:8]],
        'header_labels': [[text(node.get_text(' ', strip=True))
                           for node in header.find_all('span', recursive=False, limit=8)] for header in headers[:8]],
        'canonical_urls': [public_url(node.get('href')) for node in soup.select('head > link[rel=canonical]')[:8]],
        'og_urls': [public_url(node.get('content')) for node in soup.select('head > meta[property="og:url"]')[:8]],
        'comment_urls': [public_url(node.get('data-href')) for node in soup.select('.main-container > .fb-comments[data-href]')[:8]],
        'content_id_count': len(soup.select('#contentText')), 'sampled_native_body_count': len(native),
        'native_body_structures': [],
        'copyright_declaration_count': sum(str(value).strip() == _COPYRIGHT
            for node in soup.select('footer p') for value in node.find_all(string=True, recursive=False)),
        'labels_are_observations_not_verified_attribution': True,
        'body_text_retained': False, 'raw_response_retained': False,
        'article_identity_or_scope_granted': False, 'additional_requests': 0,
    }
    for node in native[:8]:
        elements = node.find_all(True, limit=4097)
        names = {}
        for element in elements[:4096]:
            names[element.name] = names.get(element.name, 0) + 1
        paragraphs = node.find_all(['p', 'blockquote'], limit=33)
        result['native_body_structures'].append({
            'sampled_element_count': min(len(elements), 4096), 'elements_truncated': len(elements) > 4096,
            'tag_counts': dict(sorted(names.items())[:64]), 'tag_names_truncated': len(names) > 64,
            'unrecognized_tag_names': sorted(set(names) - _BODY_TAGS)[:64],
            'sampled_reprint_prefix_present': any(re.match(
                r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', part.get_text(' ', strip=True))
                for part in paragraphs[:32]),
            'reprint_prefix_sample_truncated': len(paragraphs) > 32,
            'diagnostic_only_not_body_acceptance': True,
        })
    return result


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if getattr(parent, 'attrs', None) and set(parent.get('class', [])).intersection(
                {'hidden', 'sr-only', 'screen-reader-text', 'd-none'}):
            raise NewsError('article_identity_not_verified')


def pinview_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Bind one native region to its actual URL, title, credit and clock."""
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2}:
        raise NewsError('article_identity_not_verified')
    if pinview_article_id(page_url) is None:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, '.web-layout > .main-container > .article')
    header = _one(scope, ':scope > .article-title')
    heading = _one(header, ':scope > h1')
    title = heading.get_text(' ', strip=True)
    if (not title or len(soup.select('h1')) != 1
            or _one(soup, 'head > title').get_text(' ', strip=True) != title
            or _one(soup, 'head > meta[property="og:title"]').get('content', '').strip() != title
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'
            or _one(soup, 'head > meta[property="og:site_name"]').get('content') != '品觀點'):
        raise NewsError('article_identity_not_verified')
    canonicals = soup.select('head > link[rel=canonical]')
    if canonicals and (len(canonicals) != 1 or canonicals[0].get('href') != page_url):
        raise NewsError('article_identity_not_verified')
    comment = _one(soup, '.main-container > .fb-comments[data-href]')
    if comment.get('data-href') != page_url:
        raise NewsError('article_identity_not_verified')
    labels = header.find_all('span', recursive=False)
    if len(labels) != 2 or any(node.find(True) for node in labels):
        raise NewsError('article_attribution_not_verified')
    clock_node, credit = labels
    signature = credit.get_text(' ', strip=True)
    reviewed_credits = {_CREDIT: '李錦奇'}
    if adapter_contract_version >= 2:
        reviewed_credits[_V2_CREDIT] = '林憲祥'
    if signature not in reviewed_credits:
        raise NewsError('article_rights_or_scope_excluded' if signature else 'article_attribution_not_verified')
    published = clock_node.get_text(' ', strip=True)
    try:
        if not _CLOCK.fullmatch(published):
            raise ValueError('unreviewed visible clock precision')
        datetime.strptime(published, '%Y/%m/%d %H:%M:%S')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    if len(soup.select('#contentText')) > 1:
        raise NewsError('article_identity_not_verified')
    native = _one(scope, ':scope > div#contentText', 'body_not_found')
    for node in (scope, header, heading, comment, clock_node, credit, native):
        _native_visible(node)
    for node in native.find_all(True):
        if node.name not in _BODY_TAGS:
            raise NewsError('article_identity_not_verified')
        _native_visible(node)
    for node in native.find_all(['p', 'blockquote']):
        if re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', node.get_text(' ', strip=True)):
            raise NewsError('article_rights_or_scope_excluded')
    # The real footer paragraph also contains contacts and a privacy link.
    # Match its one direct text declaration, not the whole paragraph nor a
    # page-wide substring that could belong to an unrelated recommendation.
    copyright_nodes = [text for node in soup.select('footer p')
                       for text in node.find_all(string=True, recursive=False)
                       if str(text).strip() == _COPYRIGHT]
    if len(copyright_nodes) != 1:
        raise NewsError('article_rights_not_verified')
    _native_visible(copyright_nodes[0].parent)
    notices = [
        {'kind': 'native_visible_article_clock', 'text': published,
         'visible_precision': 'seconds', 'timezone_declared': False,
         'release_or_revision_label_declared': False},
        {'kind': 'publisher_copyright_notice', 'text': _COPYRIGHT,
         'open_license_or_bulk_permission_inferred': False},
    ]
    for image in list(native.select('img')):
        notices.append({'kind': 'unfetched_inline_image', 'media_requested': False,
                        'image_text_or_ownership_inferred': False})
        image.decompose()
    normalized = soup.new_tag('div', attrs={'class': 'pinview-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    body_scope = soup.new_tag('div')
    body_scope.append(normalized)
    return {
        'headline': title, 'author': [{'name': reviewed_credits[signature]}],
        'author_attribution_detail_json': json.dumps({'native_credit': signature,
            'role': 'publisher_declared_senior_reporter',
            'reporter_real_identity_independently_verified': False}, ensure_ascii=False),
        'datePublished': published,
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': 'publisher_current_visible_article_clock_seconds; '
            'timezone_and_release_revision_label_undeclared; no_offset_or_revision_inferred; '
            'not_observed_historical_PIT',
        'article_identity_basis': 'literal_final_URL_same_OG_and_comment_URL_single_native_heading_head_title_and_reviewed_visible_credit_v1',
        'body_completeness': 'current_native_contentText_ordered_written_prose_tables_once; '
            'media_unfetched_not_OCRed; hero_navigation_related_loops_excluded; not_original_historical_version',
    }, body_scope
