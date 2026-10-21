"""Watchmedia's native written posts; no HTTP, discovery or alias generation.

The Elementor single-page wrapper contains a header, one rich-text body and
unrelated recommendation loops. Only the first two belong to this article.
Current publisher clock/credit declarations are not historical PIT witnesses.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


WATCHMEDIA_ORIGIN = 'https://www.watchmedia01.com'
WATCHMEDIA_PATH_PATTERN = r'^/(?:archives/[1-9][0-9]*|bnews-[0-9]{14}[.]html)$'
WATCHMEDIA_BODY_SELECTOR = ':scope > .watchmedia-native-body'
_TITLE = ':scope > .elementor-element > .e-con-inner > .elementor-widget-theme-post-title > .elementor-widget-container > h1'
_BODY = ':scope > .elementor-element > .e-con-inner > article > .elementor-widget-theme-post-content > .elementor-widget-container'
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00')
_BODY_TAGS = frozenset({'p', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'span', 'a', 'b', 'strong',
    'i', 'em', 'u', 's', 'del', 'br', 'hr', 'sub', 'sup', 'ul', 'ol', 'li', 'blockquote',
    'pre', 'code', 'figure', 'figcaption', 'img', 'table', 'caption', 'colgroup', 'col',
    'thead', 'tbody', 'tfoot', 'tr', 'th', 'td'})


def watchmedia_validation_metadata(soup, *, page_url):
    """Describe bounded native declarations, never prose or an acceptance.

    Reuse the already received document. A failed exact identity comparison
    must remain diagnosable without changing source scope, retry or budgets.
    Unknown schema/body values and query/credential values are not exported.
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
                       if parts.scheme == 'https' and parts.netloc == 'www.watchmedia01.com'
                       and not parts.username and not parts.password else None)
        except ValueError:
            display = None
        return {'display_url': display, 'sha256': hashlib.sha256(value.encode()).hexdigest()}

    scopes = soup.select('body > div.elementor-location-single')
    headings = [node for scope in scopes for node in scope.select(_TITLE)]
    credits = [node for scope in scopes for node in scope.select(
        '.elementor-widget-post-info [itemprop=author]')]
    scripts = soup.select('script[type="application/ld+json"]')
    schema_error = False
    try:
        objects = _declarations(soup)
        if len(objects) > 2048:
            raise NewsError('unbounded_validation_metadata')
    except NewsError:
        objects, schema_error = [], True
    articles = [obj for obj in objects if obj.get('@type') == 'NewsArticle']
    webpages = [obj for obj in objects if obj.get('@type') == 'WebPage']
    crumbs = [obj for obj in objects if obj.get('@type') == 'BreadcrumbList']
    safe_clock = lambda value: value if isinstance(value, str) and _CLOCK.fullmatch(value) else None
    body_containers = [node for scope in scopes for node in scope.select(_BODY)]
    result = {
        'contract_version': 2, 'current_native_url_recognized': watchmedia_article_id(page_url) is not None,
        'single_scope_count': len(scopes), 'heading_count': len(headings),
        'headings': [text(node.get_text(' ', strip=True)) for node in headings[:8]],
        'post_id_classes': [[value for value in scope.get('class', [])
                             if re.fullmatch(r'post-[0-9]{1,20}', value)][:8] for scope in scopes[:8]],
        'og_titles': [text(node.get('content')) for node in soup.select('head > meta[property="og:title"]')[:8]],
        'schema_script_count': len(scripts), 'schema_error': schema_error,
        'article_schema_count': len(articles), 'webpage_schema_count': len(webpages),
        'breadcrumb_schema_count': len(crumbs),
        'articles': [{
            'id': public_url(obj.get('@id')), 'headline': text(obj.get('headline')), 'name': text(obj.get('name')),
            'published_raw': safe_clock(obj.get('datePublished')), 'modified_raw': safe_clock(obj.get('dateModified')),
            'body_declared': bool(obj.get('articleBody')), 'author_type': type(obj.get('author')).__name__,
            'author_name': text(obj.get('author', {}).get('name')) if isinstance(obj.get('author'), dict) else None,
            'explicitly_not_free': str(obj.get('isAccessibleForFree', 'true')).lower() == 'false',
        } for obj in articles[:8]],
        'webpages': [{'id': public_url(obj.get('@id')), 'url': public_url(obj.get('url')),
                      'name': text(obj.get('name'))} for obj in webpages[:8]],
        'breadcrumbs': [], 'visible_credit_count': len(credits),
        'visible_credits': [text(node.get_text(' ', strip=True)) for node in credits[:8]],
        'body_container_count': len(body_containers),
        'native_body_structures': [],
        'visible_credit_scope': 'all post-info credits inside single-page scope, including related loops; not own-author acceptance',
        'body_text_retained': False, 'raw_response_retained': False,
        'article_identity_or_scope_granted': False, 'additional_requests': 0,
    }
    for native in body_containers[:8]:
        nodes = native.find_all(True, limit=4097)
        names = {}
        for node in nodes[:4096]:
            names[node.name] = names.get(node.name, 0) + 1
        result['native_body_structures'].append({
            'sampled_element_count': min(len(nodes), 4096), 'elements_truncated': len(nodes) > 4096,
            'tag_counts': dict(sorted(names.items())[:64]),
            'tag_names_truncated': len(names) > 64,
            'unrecognized_tag_names': sorted(set(names) - _BODY_TAGS)[:64],
            'diagnostic_only_not_body_acceptance': True})
    for obj in crumbs[:8]:
        trail = obj.get('itemListElement')
        last = trail[-1] if isinstance(trail, list) and trail else None
        item = last.get('item') if isinstance(last, dict) else None
        result['breadcrumbs'].append({'item_count': len(trail) if isinstance(trail, list) else None,
            'last_item_type': type(item).__name__,
            'last_url': public_url(item.get('@id')) if isinstance(item, dict) else None,
            'last_name': text(item.get('name')) if isinstance(item, dict) else None})
    return result


def watchmedia_article_id(url, *, requested=False):
    """Recognize only literal known routes; never derive a date or a new ID."""
    if not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.watchmedia01.com'
            or parts.query or parts.fragment):
        return None
    match = re.fullmatch(r'/archives/([1-9][0-9]*)', parts.path)
    if match:
        return match[1]
    if requested and re.fullmatch(r'/bnews-[0-9]{14}[.]html', parts.path):
        return 'literal_legacy_route'
    return None


def watchmedia_response_identity(request_url, response, title, metadata_title):
    """One actual legacy301 + original metadata title, not a generated alias.

    The old URL embeds a clock-like string that is NOT used as release time.
    Native numeric IDs must never silently redirect to another numeric story.
    """
    original = watchmedia_article_id(request_url, requested=True)
    target = watchmedia_article_id(response.url)
    if original is None or target is None:
        raise NewsError('article_identity_not_verified')
    hops = getattr(response, 'redirects', ())
    if request_url == response.url and not hops:
        return 'requested_public_url'
    if original != 'literal_legacy_route' or len(hops) != 1:
        raise NewsError('article_identity_not_verified')
    hop = hops[0]
    known_title = metadata_title if isinstance(metadata_title, str) else ''
    for suffix in (' - watchmedia01.com', ' - 觀傳媒'):
        if known_title.endswith(suffix):
            known_title = known_title[:-len(suffix)]
            break
    if (hop.status != 301 or hop.from_url != request_url or hop.to_url != response.url
            or hop.location != response.url or not known_title
            or re.sub(r'\s+', '', known_title) != re.sub(r'\s+', '', title)):
        raise NewsError('article_identity_not_verified')
    return 'verified_watchmedia_literal_legacy_source301_and_metadata_title_v1; original_frontier_url_retained'


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('competing source declaration keys')
        result[key] = value
    return result


def _declarations(soup):
    scripts = soup.select('script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 16:
        raise NewsError('article_identity_not_verified')
    objects = []
    try:
        for script in scripts:
            text = script.get_text()
            if len(text) > 128 << 10:
                raise ValueError('unbounded source declaration')
            objects.extend(_objects(json.loads(text, object_pairs_hook=_unique_object)))
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    return objects


def _one_object(objects, kind, *, identity=None, error='article_identity_not_verified'):
    matches = [obj for obj in objects if obj.get('@type') == kind
               and (identity is None or obj.get('@id') == identity)]
    if len(matches) != 1:
        raise NewsError(error)
    return matches[0]


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if not getattr(parent, 'attrs', None):
            continue
        if set(parent.get('class', [])).intersection({'hidden', 'screen-reader-text', 'sr-only',
                'elementor-hidden-desktop', 'elementor-hidden-tablet', 'elementor-hidden-mobile'}):
            raise NewsError('article_identity_not_verified')


def watchmedia_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Bind current native prose to exact URL, title, own credit and clocks."""
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3}:
        raise NewsError('article_identity_not_verified')
    title_equivalences = []

    def same_title(observed, expected, field):
        if observed == expected:
            return True
        # Only comparison whitespace changes. Do not remove token boundaries,
        # normalize punctuation/digits/case or rewrite any original value.
        if (adapter_contract_version >= 2 and isinstance(observed, str)
                and re.sub(r'\s+', ' ', observed).strip() == re.sub(r'\s+', ' ', expected).strip()):
            title_equivalences.append({'kind': 'native_title_whitespace_equivalence',
                'field': field, 'source_raw': observed, 'visible_reference_raw': expected,
                'comparison_rule': 'unicode_whitespace_runs_to_one_ASCII_space_and_strip_edges',
                'comparison_only_original_values_retained': True})
            return True
        if adapter_contract_version == 3 and isinstance(observed, str):
            # This native WordPress post exposes three literal representations
            # of one ellipsis across H1, SEO and JSON breadcrumbs. Decode only
            # that observed decimal entity / isolated three-dot sequence, never
            # general HTML entities, punctuation, digits or numeric separators.
            def ellipsis_comparison(value):
                value = re.sub(r'\s+', ' ', value).strip().replace('&#8230;', '…')
                return re.sub(r'(?<![.])[.]{3}(?![.])', '…', value)

            if ellipsis_comparison(observed) == ellipsis_comparison(expected):
                title_equivalences.append({'kind': 'native_title_ellipsis_representation_equivalence',
                    'field': field, 'source_raw': observed, 'visible_reference_raw': expected,
                    'comparison_rule': 'unicode_whitespace_runs_and_literal_decimal_8230_or_isolated_three_ASCII_dots_to_ellipsis',
                    'comparison_only_original_values_retained': True})
                return True
        return False

    identity = watchmedia_article_id(page_url)
    if identity is None:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'body > div.elementor-location-single')
    post_ids = [value for value in scope.get('class', []) if re.fullmatch(r'post-[0-9]+', value)]
    if post_ids != ['post-' + identity] or scope.get('data-elementor-type') != 'single-page':
        raise NewsError('article_identity_not_verified')
    heading = _one(scope, _TITLE)
    title = heading.get_text(' ', strip=True)
    seo_title = title + ' - 觀傳媒'
    if (not title or _one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'
            or _one(soup, 'head > meta[property="og:site_name"]').get('content') != '觀傳媒'
            or not same_title(_one(soup, 'head > meta[property="og:title"]').get('content'), seo_title, 'og:title')):
        raise NewsError('article_identity_not_verified')
    objects = _declarations(soup)
    declared = _one_object(objects, 'NewsArticle')
    webpage = _one_object(objects, 'WebPage')
    organization_id = WATCHMEDIA_ORIGIN + '/#organization'
    organization = _one_object(objects, 'Organization', identity=organization_id)
    page_ref = {'@id': page_url + '#webpage'}
    if (declared.get('@id') != page_url + '#richSnippet'
            or not same_title(declared.get('headline'), seo_title, 'NewsArticle.headline')
            or not same_title(declared.get('name'), seo_title, 'NewsArticle.name')
            or declared.get('mainEntityOfPage') != page_ref or declared.get('isPartOf') != page_ref
            or declared.get('publisher') != {'@id': organization_id}
            or organization.get('name') != '觀傳媒'
            or webpage.get('@id') != page_ref['@id'] or webpage.get('url') != page_url
            or not same_title(webpage.get('name'), seo_title, 'WebPage.name') or declared.get('articleBody')):
        raise NewsError('article_identity_not_verified')
    # A second current/related Article must not shadow the single NewsArticle.
    for obj in objects:
        kinds = obj.get('@type', [])
        kinds = [kinds] if isinstance(kinds, str) else kinds
        if (isinstance(kinds, list) and obj is not declared
                and any(kind in ('NewsArticle', 'Article', 'BlogPosting', 'ReportageNewsArticle') for kind in kinds)):
            raise NewsError('article_identity_not_verified')
    if str(declared.get('isAccessibleForFree', 'true')).lower() == 'false':
        raise NewsError('paywall_or_login')
    crumb = _one_object(objects, 'BreadcrumbList')
    trail = crumb.get('itemListElement')
    if (crumb.get('@id') != page_url + '#breadcrumb' or not isinstance(trail, list)
            or len(trail) != 3 or not isinstance(trail[-1], dict)
            or not isinstance(trail[-1].get('item'), dict)
            or set(trail[-1]['item']) != {'@id', 'name'}
            or trail[-1]['item'].get('@id') != page_url
            or not same_title(trail[-1]['item'].get('name'), title, 'BreadcrumbList.last.name')):
        raise NewsError('article_identity_not_verified')
    header = heading.parent.parent.parent
    byline = _one(header, '.elementor-widget-post-info [itemprop=author]', 'article_attribution_not_verified')
    link = _one(byline, ':scope > a[href]', 'article_attribution_not_verified')
    label = _one(link, ':scope > .elementor-post-info__item--type-author', 'article_attribution_not_verified')
    prefix = _one(label, ':scope > .elementor-post-info__item-prefix', 'article_attribution_not_verified')
    name = ' '.join(str(text).strip() for text in label.find_all(string=True, recursive=False) if str(text).strip())
    author = declared.get('author')
    profile = link.get('href', '')
    if (prefix.get_text(' ', strip=True) != '記者 /' or not name or not isinstance(author, dict)
            or author != {'@id': profile, 'name': name}
            or not re.fullmatch(re.escape(WATCHMEDIA_ORIGIN) + r'/author/[A-Za-z0-9_-]+', profile)):
        raise NewsError('article_attribution_not_verified')
    # The CMS also labels partner organizations as '記者'. A generic Person
    # schema alone therefore cannot turn their credit into our own reporter.
    if (not re.fullmatch(r'[\u3400-\u9fff]+(?: [\u3400-\u9fff]+)*', name)
            or not 2 <= len(name.replace(' ', '')) <= 6
            or re.search(r'新聞|傳媒|媒體|報|網|社|編輯|中心|團隊|小編', name)):
        raise NewsError('article_rights_or_scope_excluded')
    person = _one_object(objects, 'Person', identity=profile, error='article_attribution_not_verified')
    if (person.get('name') != name or person.get('url') != profile
            or person.get('worksFor') != {'@id': organization_id}):
        raise NewsError('article_attribution_not_verified')
    clock_node = _one(header, '.elementor-widget-post-info [itemprop=datePublished] time',
                      'invalid_bulk_publication_clock')
    clocks = {}
    for field, property_name in [('datePublished', 'article:published_time'), ('dateModified', 'article:modified_time')]:
        raw = declared.get(field)
        try:
            if not isinstance(raw, str) or not _CLOCK.fullmatch(raw):
                raise ValueError('unverified native clock precision')
            clocks[field] = datetime.fromisoformat(raw)
        except ValueError:
            raise NewsError('invalid_bulk_publication_clock') from None
        metas = soup.select(f'head > meta[property="{property_name}"]')
        # Unedited native posts can omit modified_time in the head. Both
        # current schema fields still declare that revision explicitly; do
        # not synthesize it from the release or reject a valid absent meta.
        if ((len(metas) != 1 if field == 'datePublished' else len(metas) > 1)
                or any(meta.get('content') != raw for meta in metas) or webpage.get(field) != raw):
            raise NewsError('invalid_bulk_publication_clock')
    if (clocks['dateModified'] < clocks['datePublished']
            or clock_node.get_text(' ', strip=True) != clocks['datePublished'].date().isoformat()):
        raise NewsError('invalid_bulk_publication_clock')
    native = _one(scope, _BODY, 'body_not_found')
    for node in (scope, heading, byline, link, label, prefix, clock_node, native):
        _native_visible(node)
    for node in native.find_all(True):
        if node.name not in _BODY_TAGS:
            raise NewsError('article_identity_not_verified')
        _native_visible(node)
    for node in native.find_all(['p', 'blockquote']):
        if re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', node.get_text(' ', strip=True)):
            raise NewsError('article_rights_or_scope_excluded')
    captions, notices = [], [{'kind': 'native_visible_publication_day',
        'text': clock_node.get_text(' ', strip=True), 'visible_precision': 'date_only',
        'source_machine_seconds_and_offset_not_inferred_from_visible_day': True}]
    notices.extend(title_equivalences)
    for figure in list(native.select('figure')):
        if figure.find('img') is None:
            continue  # A text/table figure is prose, not a photo.
        if (figure.find(['table', 'p', 'blockquote', 'ul', 'ol', 'h2', 'h3']) is not None
                or len(figure.select('figcaption')) > 1
                or any(str(text).strip() for text in figure.find_all(string=True)
                       if text.find_parent('figcaption') is None)):
            raise NewsError('article_identity_not_verified')
        captions.extend(node.get_text(' ', strip=True) for node in figure.select('figcaption')
                        if node.get_text(' ', strip=True))
        notices.append({'kind': 'unfetched_native_image_figure', 'media_requested': False,
                        'image_text_or_ownership_inferred': False})
        figure.decompose()
    for image in list(native.select('img')):
        notices.append({'kind': 'unfetched_inline_image', 'media_requested': False,
                        'image_text_or_ownership_inferred': False})
        image.decompose()
    normalized = soup.new_tag('div', attrs={'class': 'watchmedia-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    body_scope = soup.new_tag('div')
    body_scope.append(normalized)
    return {
        'headline': title, 'author': [{'name': name}],
        'author_attribution_detail_json': json.dumps({'native_credit': label.get_text(' ', strip=True),
            'schema_profile': profile, 'role': 'publisher_declared_reporter',
            'reporter_real_identity_independently_verified': False}, ensure_ascii=False),
        'datePublished': declared['datePublished'], 'dateModified': declared['dateModified'],
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': 'publisher_current_NewsArticle_WebPage_exact_seconds_offset; '
            'release_head_crosscheck_revision_head_checked_if_present; '
            'visible_publication_day_separate; not_observed_historical_PIT',
        'article_identity_basis': 'literal_final_URL_native_post_ID_canonical_OG_single_NewsArticle_WebPage_breadcrumb_heading_and_visible_credit_v1',
        'body_completeness': 'current_native_written_post_prose_tables_once; native_image_figures_captions_separate; '
            'media_unfetched_not_OCRed; unrelated_loops_excluded; not_original_historical_version',
    }, body_scope
