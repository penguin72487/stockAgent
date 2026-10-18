"""FTNN native article proof; HTTP, scheduling and persistence stay shared."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

from bs4 import Comment, NavigableString, Tag

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


FTNN_BODY_SELECTOR = ':scope > .news-body'
_AD_LABEL = '廣告 更多內容請繼續往下閱讀'
_EDITOR_SECTION_ATTRIBUTES = frozenset({'data-scroll-anchor', 'data-testid', 'data-turn',
    'data-turn-id', 'data-turn-id-container', 'dir'})
# Entire native data URI in the retained-structure observation of 520952.
# This is a hidden nontext asset, NOT an empty span or invented article text.
_HIDDEN_INLINE_ASSET = (246, 'f9a61a7f9aacc1db170363075e92849812736e36f86ccda3ff16c166d6b90c04')


def _preserve_hidden_inline_assets(body):
    """Move only one reviewed hidden image shape into lossless source notices.

    The original document stays untouched, including its entire data URI.
    Notices bind its URI hash/length to that raw response, with both elements'
    other attributes; do not duplicate/decode/export a media binary as prose.
    Unknown payloads/attributes, hidden prose and hidden ancestors still fail
    the normal visibility guard. No image request, rendering or OCR is used.
    """
    notices = []
    for wrapper in list(body.find_all('span')):
        if (wrapper.attrs != {'lang': 'EN-US', 'style': 'display:none'}
                or len(wrapper.contents) != 1):
            continue
        image = wrapper.contents[0]
        if (not isinstance(image, Tag) or image.name != 'img' or image.contents
                or set(image.attrs) != {'alt', 'src', 'style'} or image.get('alt') != ''
                or image.get('style') != 'width:15px; height:15px'):
            continue
        src = image.get('src')
        if (not isinstance(src, str) or len(src) != _HIDDEN_INLINE_ASSET[0]
                or not re.fullmatch(r'data:image/[a-zA-Z0-9.+-]+;base64,[A-Za-z0-9+/]+=*', src)
                or hashlib.sha256(src.encode()).hexdigest() != _HIDDEN_INLINE_ASSET[1]):
            continue
        _visible(wrapper.parent)
        notices.append({'kind': 'native_hidden_inline_image_preserved_v1', 'text': '',
            'wrapper_attributes': dict(wrapper.attrs),
            'image_attributes': {key: value for key, value in image.attrs.items() if key != 'src'},
            'source_uri_basis': 'same_article_raw_response_native_hidden_image_src',
            'source_uri_characters': len(src), 'source_uri_sha256': hashlib.sha256(src.encode()).hexdigest(),
            'source_uri_media_type': src.partition(',')[0],
            'source_display_state': 'display:none', 'visible_body_text_invented': False})
        wrapper.decompose()
    return notices


def _remove_empty_hidden_spans(body):
    """Ignore only text-free leaf display plumbing on the isolated body copy.

    A span with prose, descendants or semantic attributes is not empty.
    Hidden ancestors still fail: this is not permission to reveal or drop
    hidden article text, figures, credits, source notices or nested widgets.
    """
    for node in list(body.find_all('span')):
        if (set(node.attrs) == {'style'}
                and re.fullmatch(r'display\s*:\s*none\s*;?', node['style'].strip(), re.I)
                and not node.find(True) and not node.get_text(strip=True)):
            _visible(node.parent)
            node.decompose()


def _native_flow(body, *, adapter_contract_version):
    """Reviewed native editor layouts, not arbitrary descendant searching.

    Preserve the entire .news-body, including prose outside a report section,
    embeds and outer notices. Only the reporter/ad location changes. Unknown
    prefixes, nested sections and hidden containers remain errors; never
    search later paragraphs for an author.
    """
    if adapter_contract_version < 3 or not body.find('section', recursive=False):
        return body
    children = list(body.children)
    elements = [node for node in children if getattr(node, 'name', None)]
    if any(str(node).strip() for node in children
           if not getattr(node, 'name', None) and not isinstance(node, Comment)):
        raise NewsError('article_attribution_not_verified')
    shape = tuple(node.name for node in elements)
    empty = None
    section_attributes = _EDITOR_SECTION_ATTRIBUTES
    if shape == ('section', 'section', 'p'):
        empty, flow = elements[:2]
    elif adapter_contract_version >= 6 and shape in {
            ('section', 'iframe', 'p'), ('section', 'p')}:
        flow = elements[0]
        if shape == ('section', 'p'):
            section_attributes = _EDITOR_SECTION_ATTRIBUTES - {'data-scroll-anchor'}
    elif adapter_contract_version >= 6 and shape in {
            ('section', 'section', 'div', 'div', 'p'),
            ('section', 'section', 'figure', 'p', 'p', 'div', 'div', 'p', 'p', 'p', 'div', 'div', 'p')}:
        # In one original the section contains only the opening paragraphs;
        # the remaining report stays outside it. It MUST NOT be discarded.
        empty, flow = elements[:2]
    else:
        raise NewsError('article_attribution_not_verified')
    for section in ([empty, flow] if empty is not None else [flow]):
        if (set(section.attrs) != section_attributes
                or section.get('dir') not in {'auto', 'ltr', 'rtl'} or section.find('section')):
            raise NewsError('article_attribution_not_verified')
        _visible(section)
    if empty is not None and (empty.find(True) or empty.get_text(strip=True)):
        raise NewsError('article_attribution_not_verified')
    return flow


def _native_byline(body, *, adapter_contract_version=1):
    body = _native_flow(body, adapter_contract_version=adapter_contract_version)
    if adapter_contract_version == 1:
        return _one(body, ':scope > p:first-child', 'article_attribution_not_verified')
    byline = _one(body, ':scope > p:first-of-type', 'article_attribution_not_verified')
    preceding = list(byline.previous_siblings)
    elements = [node for node in preceding if getattr(node, 'name', None)]
    if (any(str(node).strip() for node in preceding
            if not getattr(node, 'name', None) and not isinstance(node, Comment))
            or len(elements) > 1):
        raise NewsError('article_attribution_not_verified')
    if elements:
        photo = elements[0]
        if adapter_contract_version >= 6 and photo.name == 'meta':
            # The observed leading native UTF-8 declaration has no text or
            # descendants. Keep it in raw/cleaned HTML; it is not a photo or
            # an earlier paragraph. Refresh/http-equiv/extra attributes,
            # other encodings and unknown prefixes are not this layout.
            if (set(photo.attrs) != {'charset'} or not isinstance(photo.get('charset'), str)
                    or photo['charset'].lower() != 'utf-8' or photo.contents):
                raise NewsError('article_attribution_not_verified')
            _visible(photo)
            return byline
        if photo.name != 'figure' or photo.get('class') != ['image'] or not photo.find('img'):
            raise NewsError('article_attribution_not_verified')
        _visible(photo)
        # The only text permitted before the credit belongs to the source's
        # image caption. Never drop a paragraph or move a later reporter quote
        # into the article's attribution slot.
        if any(str(text).strip() and text.find_parent('figcaption') is None for text in photo.strings):
            raise NewsError('article_attribution_not_verified')
        if photo.select('script, article, form, iframe, [rel~="license"]'):
            raise NewsError('article_attribution_not_verified')
    return byline


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


def ftnn_verified_short_prose(body, text, *, headline):
    """A whole native flash is not missing merely because it is under 100 chars.

    Only called AFTER the complete native free/original/reporter/clock proof.
    Reviewed short reports have direct plain paragraphs and optional original
    figures. Match all retained text, never accept a caption, link, headline,
    risk reminder or media placeholder as the report. Nothing is removed or
    rewritten; captions and the unchanged source are still stored separately.
    """
    if not isinstance(text, str) or not 0 < len(text) < 100:
        return False
    paragraphs = []
    for node in body.children:
        if not isinstance(node, Tag):
            if str(node).strip():
                return False
            continue
        if node.name == 'figure' and node.attrs == {'class': ['image']}:
            continue
        if node.name != 'p' or node.attrs:
            return False
        descendants = node.find_all(True)
        if descendants:
            # The observed flash ends in <p><br>&nbsp;</p>. A single bare BR
            # is empty editor padding, not inline prose or a hidden payload.
            if (len(descendants) != 1 or descendants[0].name != 'br'
                    or descendants[0].attrs or descendants[0].contents
                    or any(child is not descendants[0] and
                           (type(child) is not NavigableString or child.strip())
                           for child in node.contents)):
                return False
            continue
        value = node.get_text('\n', strip=True)
        if value:
            paragraphs.append(value)
    if '\n'.join(paragraphs) != text:
        return False
    compact = lambda value: re.sub(r'\s+', '', value).rstrip('。！？.!?')
    if not paragraphs or compact(text) == compact(headline):
        return False
    notice = re.compile(r'^(?:◎|※|《FTNN新聞網》|FTNN新聞網提醒|延伸閱讀|相關閱讀|更多新聞|'
                        r'投資風險|免責聲明|風險提醒|資料僅供參考)')
    return any(not notice.match(value) and re.search(r'[\u3400-\u9fff]', value)
               and re.search(r'[。！？!?]$', value) for value in paragraphs)


def ftnn_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Use the whole visible report, not description, ads or related stories.

    The source's NewsArticle has no URL/articleBody. Bind its single schema
    to canonical/OG/breadcrumb, the visible headline, reporter and release
    minute instead. A public reporter account is attribution evidence, not a
    grant of third-party rights or proof of historical point-in-time release.
    """
    if type(adapter_contract_version) is not int or adapter_contract_version not in (1, 2, 3, 4, 5, 6):
        raise NewsError('article_identity_not_verified')
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
    last_item = (crumbs[-1].get('item') if isinstance(crumbs, list) and crumbs
                 and isinstance(crumbs[-1], dict) else None)
    breadcrumb_matches = last_item == {'@id': page_url, 'name': title}
    if adapter_contract_version >= 2:
        # Native schema/breadcrumb titles can retain trailing full-width space
        # which get_text(strip=True) removes from the visible heading. Normalize
        # only the outer whitespace; URLs, keys, punctuation and inner spaces
        # must still agree. Original HTML remains byte-for-byte in the receipt.
        breadcrumb_matches = (isinstance(last_item, dict) and set(last_item) == {'@id', 'name'}
                              and last_item['@id'] == page_url and isinstance(last_item['name'], str)
                              and last_item['name'].strip() == title)
    publisher = declared.get('publisher')
    if (not isinstance(crumbs, list) or not crumbs or not isinstance(crumbs[-1], dict)
            or not breadcrumb_matches
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
    byline = _native_byline(body, adapter_contract_version=adapter_contract_version)
    author = declared.get('author')
    if (not isinstance(author, dict) or author.get('@type') != 'Person'
            or not isinstance(author.get('name'), str) or not author['name'].strip()
            or not re.fullmatch(r'https://www[.]ftnn[.]com[.]tw/search\?author=a[0-9]+', str(author.get('url', '')))):
        raise NewsError('article_attribution_not_verified')
    credit = byline.get_text(' ', strip=True)
    role = r'(?:實習)?記者' if adapter_contract_version >= 2 else '記者'
    if not re.fullmatch(r'(?:FTNN新聞網)?' + role + re.escape(compact(author['name'])) + r'／[^／\s]{1,16}報導', compact(credit)):
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
    _native_byline(clean_body, adapter_contract_version=adapter_contract_version).decompose()
    clean_flow = _native_flow(clean_body, adapter_contract_version=adapter_contract_version)
    ad_scopes = (clean_flow, clean_body) if adapter_contract_version >= 6 and clean_flow is not clean_body else (clean_flow,)
    for node in [node for ad_scope in ad_scopes for node in ad_scope.select(':scope > p')]:
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
    if adapter_contract_version >= 3:
        _remove_empty_hidden_spans(clean_body)
    hidden_assets = _preserve_hidden_inline_assets(clean_body) if adapter_contract_version >= 4 else []
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
        'verified_article_notices_json': json.dumps(clock_notices + hidden_assets, ensure_ascii=False),
        'article_identity_basis': ('canonical_OG_outer_whitespace_breadcrumb_single_schema_native_reporter_visible_clocks_inert_UTF8_prefix_and_complete_editor_flow_v7'
            if adapter_contract_version >= 6 else 'canonical_OG_outer_whitespace_breadcrumb_single_schema_native_reporter_visible_clocks_short_native_prose_and_preserved_asset_v6'
            if adapter_contract_version >= 5 else 'canonical_OG_outer_whitespace_breadcrumb_single_schema_native_reporter_visible_clocks_native_hidden_inline_asset_preserved_v5'
            if adapter_contract_version >= 4 else 'canonical_OG_outer_whitespace_breadcrumb_single_schema_native_reporter_visible_clocks_empty_leaf_spans_and_editor_sections_v4'
            if adapter_contract_version >= 3 else 'canonical_OG_outer_whitespace_breadcrumb_single_schema_heading_native_reporter_and_visible_clocks_v3'
            if adapter_contract_version == 2 else 'canonical_OG_breadcrumb_single_schema_heading_reporter_and_visible_release_revision_minutes_v2'
            if dual_clock else 'canonical_OG_breadcrumb_single_schema_heading_reporter_and_visible_release_minute_v1'),
        'body_completeness': ('verified_current_whole_native_report; source_risk_notice_retained; advertising_and_related_stories_excluded; captions_recorded_separately'
                              + ('; native_hidden_inline_asset_preserved_separately' if hidden_assets else '')),
    }, cleaned
