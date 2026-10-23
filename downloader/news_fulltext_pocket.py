"""Pocket School's reviewed public editorial text, not another HTTP client.

The CMS nests an article inside an article. Select the native body once and
keep its tables/FAQ; do not concatenate every ``article`` or a brokerage CTA.
Current schema release/revision declarations and the date-only visible
``更新於`` label are different evidence, never interchangeable PIT clocks.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from urllib.parse import parse_qsl, urljoin, urlsplit

from downloader.news_fulltext_policy import NewsError, _objects, _plain_text_node
from downloader.news_fulltext_public_originals import _one, _visible


POCKET_ORIGIN = 'https://www.pocket.tw'
POCKET_BODY_SELECTOR = ':scope > .pocket-native-body'
POCKET_PATH_PATTERN = '^/school/(?:report/[A-Za-z]+|articles)/[1-9][0-9]*/$'
POCKET_V10_PATH_PATTERN = r'^/(?:school/(?:report/[A-Za-z]+|articles)|invest_news/article)/[1-9][0-9]*/{0,2}$'
POCKET_V11_PATH_PATTERN = r'^/(?:(?:school/(?:report/[A-Za-z]+|articles)|invest_news/article)/[1-9][0-9]*/{0,2}|school/articles/article-[a-z0-9]{8}/)$'
# Full contracts derived from the installed ABI9 and reviewed ABI10 catalog,
# not permission aliases. Any other policy edit gets no inherited HTTP state.
POCKET_V10_SCOPE_REPAIR_IDENTITY = (
    '8b54651a1148431359a495eb07daa6a807158b6cfa64a06e46815baad2771a29',
    '687294216eb1ba1a4ebfc002d100bdc3310acaf14147a439b19bd94631ad4d6e',
)
POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY = (
    '6238b0d0bd9823279a7be95572d601f4679f02002b730abcbdff844551c7ff33',
    '9b9585c074555ecdb4c45cdf601216d92b4c6687c7f49ef60c42dde5ed8e9057',
)
# This extension recognizes a literal opaque School ID, not a numeric alias.
# Only the reviewed full contract pair may retain the prior HTTP measurements.
POCKET_V11_SCOPE_REPAIR_IDENTITY = (
    POCKET_V10_SCOPE_REPAIR_IDENTITY[1],
    '8c7650e6afe495c51acb9b9256cffb1d848a4d9fe2275c323c3625c5b35b393a',
)
POCKET_V11_NEW_ARTICLE_REQUESTS = tuple(
    POCKET_ORIGIN + '/school/articles/article-' + suffix + '/'
    for suffix in ('mvaoags6', 'mvep8lsc', 'mvex3yk2', 'mvexg3rz', 'mvgbav11', 'mvgcdbyd')
)
# Three exact newly retained originals, not a retry of every missing article.
POCKET_V12_NEW_BODY_REQUESTS = (
    'https://www.pocket.tw/school/articles/article-mvexg3rz/',
    'https://www.pocket.tw/school/articles/article-mvgcdbyd/',
    'https://www.pocket.tw/school/report/newflash/8013',
)
POCKET_V12_SCOPE_REPAIR_IDENTITY = (
    POCKET_V11_SCOPE_REPAIR_IDENTITY[1],
    '70c7e9e7e90d1e09ca70311793ac85f0abf712b8c15fc38358c5d7123e22890e',
)
# Actual literal responses (2026-10-21) established same-ID native School
# redirects and valid complete bodies. These twelve IDs have no local body;
# ten other observed routes already have local originals and are not replayed.
POCKET_V10_NEW_ARTICLE_REQUESTS = (
    'https://www.pocket.tw/invest_news/article/1264528',
    'https://www.pocket.tw/invest_news/article/1268889',
    'https://www.pocket.tw/invest_news/article/1278880',
    'https://www.pocket.tw/invest_news/article/1281244',
    'https://www.pocket.tw/invest_news/article/7931',
    'https://www.pocket.tw/invest_news/article/7933',
    'https://www.pocket.tw/invest_news/article/7971',
    'https://www.pocket.tw/invest_news/article/8025',
    'https://www.pocket.tw/school/report/BeforeMarket/1277930',
    'https://www.pocket.tw/school/report/BeforeMarket/1279232',
    'https://www.pocket.tw/school/report/BeforeMarket/1279997',
    'https://www.pocket.tw/school/report/SCHOOL/7973',
)
POCKET_AUTHOR = '口袋學堂編輯團隊'
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]{1,6})?Z')
_BODY_TAGS = frozenset({'p', 'h2', 'h3', 'h4', 'div', 'span', 'strong', 'b', 'em', 'i', 'u',
    's', 'a', 'br', 'hr', 'ul', 'ol', 'li', 'section', 'details', 'summary', 'blockquote',
    'table', 'caption', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'colgroup', 'col', 'img',
    'sup', 'sub'})


def _inner_calendar_notice(label, visible_day, version):
    """Keep native as-of periods and update dates distinct from release clocks.

    These are the literal forms witnessed in the native article-meta node.
    No arbitrary prose/credit is removed, no close time is invented, and an
    actual differing date is retained rather than rewritten to the outer one.
    """
    if label == f'更新於 {visible_day.isoformat()} ｜ {POCKET_AUTHOR}':
        return {'kind': 'native_repeated_update_byline', 'text': label}
    if version < 3:
        raise NewsError('invalid_bulk_publication_clock')
    numeric = r'(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})'
    slash = r'(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})'
    han = r'(?P<year>[0-9]{4})年(?P<month>[0-9]{1,2})月(?P<day>[0-9]{1,2})日'
    spaced = r'(?P<year>[0-9]{4}) 年 (?P<month>[0-9]{1,2}) 月 (?P<day>[0-9]{1,2}) 日'
    forms = (
        ('update_date', '更新於 '+numeric+' ｜ '+POCKET_AUTHOR),
        ('update_date', '更新於 '+slash+' ｜ '+POCKET_AUTHOR),
        ('update_date', '更新日期：'+han+' '+POCKET_AUTHOR),
        ('data_as_of_close_date', '資料截至 '+spaced+'收盤 ｜ '+POCKET_AUTHOR),
        ('data_as_of_date', '資料截至 '+numeric),
        ('data_as_of_date', '口袋觀點｜資料截至 '+spaced),
    )
    if version >= 12:
        forms += (('data_as_of_date', '口袋學堂｜資料截至 '+spaced),)
    for role, pattern in forms:
        match = re.fullmatch(pattern, label)
        if match is None:
            continue
        try:
            day = datetime(*(int(match[name]) for name in ('year', 'month', 'day'))).date()
        except ValueError:
            raise NewsError('invalid_bulk_publication_clock') from None
        return {'kind': 'native_inner_calendar_label', 'text': label, 'clock_role': role,
            'calendar_date': day.isoformat(), 'visible_precision': 'date_only',
            'matches_outer_visible_update_day': day == visible_day,
            'not_promoted_to_release_or_revision_clock': True,
            'close_session_does_not_supply_a_wall_clock_time': role == 'data_as_of_close_date'}
    raise NewsError('invalid_bulk_publication_clock')


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('competing native declaration keys')
        result[key] = value
    return result


def _native_visible(node, *, accessible_table_captions=False):
    _visible(node)
    for parent in [node, *node.parents]:
        if not getattr(parent, 'attrs', None):
            continue
        classes = set(parent.get('class', []))
        # Native table captions intentionally use screen-reader-only styling.
        # They are table context, not an invisible alternative article. The
        # exception is exact and leaves every actual hidden/ARIA/style gate.
        if (accessible_table_captions and parent.name == 'caption'
                and parent.parent.name == 'table' and classes == {'sr-only'}
                and not parent.find(True)):
            continue
        if classes.intersection({'hidden', 'sr-only'}):
            raise NewsError('article_identity_not_verified')


def pocket_article_id(url, *, requested=False, adapter_contract_version=1):
    """Recognize literal routes only; never synthesize a redirect or alias."""
    if not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.pocket.tw'
            or parts.query or parts.fragment):
        return None
    if requested and adapter_contract_version >= 10:
        route = r'/(?:school/(?:report/[A-Za-z]+|articles)|invest_news/article)/([1-9][0-9]*)/{0,2}'
    else:
        route = (r'/school/(?:report/[A-Za-z]+|articles)/([1-9][0-9]*)/' if requested
                 else r'/school/articles/([1-9][0-9]*)/')
    if adapter_contract_version in {11, 12}:
        route = '(?:' + route + r'|/school/articles/(article-[a-z0-9]{8})/)'
    match = re.fullmatch(route, parts.path)
    if match and adapter_contract_version in {11, 12}:
        return next(value for value in match.groups() if value is not None)
    return match[1] if match else None


def pocket_numeric_transport_verified(response, requested_url):
    """Verify the received numeric-ID chain, never request a guessed alias.

    Native final identity stays strict. The reviewed extra spellings are
    request routes only; every actual intermediate hop must remain same-ID
    and same-origin, with its literal Location, time and response witness.
    """
    from downloader.news_fulltext_http import RedirectHop
    identity = pocket_article_id(requested_url, requested=True, adapter_contract_version=10)
    if identity is None or pocket_article_id(response.url) != identity:
        return False
    hops = response.redirects
    if not isinstance(hops, tuple) or any(not isinstance(hop, RedirectHop) for hop in hops):
        return False
    if response.url == requested_url:
        return not hops
    if not 1 <= len(hops) <= 8:
        return False
    previous = requested_url
    for hop in hops:
        if (hop.from_url != previous or hop.status not in {301, 302, 303, 307, 308}
                or pocket_article_id(hop.to_url, requested=True, adapter_contract_version=10) != identity
                or not isinstance(hop.location, str)
                or not isinstance(hop.response_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', hop.response_sha256)
                or type(hop.response_bytes) is not int or not 0 <= hop.response_bytes <= 8 << 20):
            return False
        try:
            if (urljoin(hop.from_url, hop.location) != hop.to_url
                    or datetime.fromisoformat(hop.observed_at_utc).utcoffset() is None):
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        previous = hop.to_url
    return previous == response.url


def pocket_article_transport_verified(response, requested_url, *, adapter_contract_version):
    """Keep witnessed numeric redirects and literal opaque identities distinct.

    Opaque School requests have only been witnessed as self-canonical pages.
    They cannot acquire a numeric ID, another slug, or an alias via a redirect.
    Native canonical/schema/byline/body verification still follows separately.
    """
    if pocket_numeric_transport_verified(response, requested_url):
        return True
    identity = pocket_article_id(requested_url, requested=True,
        adapter_contract_version=adapter_contract_version)
    return (adapter_contract_version in {11, 12} and isinstance(identity, str)
        and identity.startswith('article-') and response.url == requested_url
        and isinstance(response.redirects, tuple) and not response.redirects)


def _own_openaccount_link(url):
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    return (parts.scheme == 'https' and parts.netloc == 'www.pocket.tw'
            and parts.path == '/openaccountonline/oa/login/new')


def pocket_clock_metadata(soup, *, page_url):
    """Finite current-failure clock evidence, never an article or author grant.

    Only literal ISO declarations and native dates/known organisation labels
    may leave the envelope. Unknown header text stays a hash/length, not a
    body excerpt. Keep inner and outer update labels separate.
    """
    facts = {'contract_version': 2, 'accepted_as_article': False,
             'raw_names_or_body_retained': False, 'publisher_requests': 0}

    def shape(value):
        if not isinstance(value, str):
            return {'type': 'missing' if value is None else 'other'}
        result = {'type': 'string', 'characters': len(value)}
        try:
            result['sha256'] = hashlib.sha256(value.encode()).hexdigest()
        except UnicodeEncodeError:
            return {**result, 'invalid_unicode_scalar': True}
        # Date facts/known site labels only, never arbitrary byline or prose.
        known_label = bool(re.fullmatch(POCKET_AUTHOR + r' 市場分析師 更新於 [0-9]{4}/[0-9]{2}/[0-9]{2}', value))
        if not known_label and 0 < len(value) <= 160:
            try:
                _inner_calendar_notice(value, datetime(2000, 1, 1).date(), 3)
            except NewsError:
                pass
            else:
                known_label = True
        if 0 < len(value) <= 160 and (known_label or _CLOCK.fullmatch(value)):
            result['literal_clock_or_known_label'] = value
        result['matches_UTC_clock_syntax'] = bool(_CLOCK.fullmatch(value))
        return result

    declarations = []
    scripts = soup.select('script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 16:
        return {**facts, 'reason': 'schema_count_not_bounded'}
    for script in scripts:
        text = script.get_text()
        if len(text) > 128 << 10:
            return {**facts, 'reason': 'schema_size_not_bounded'}
        try:
            declarations.extend(obj for obj in _objects(json.loads(text, object_pairs_hook=_unique_object))
                                if obj.get('@type') == 'Article')
        except (ValueError, TypeError, RecursionError):
            return {**facts, 'reason': 'schema_not_decoded'}
    if len(declarations) != 1:
        return {**facts, 'reason': 'single_Article_required', 'article_count': len(declarations)}
    article = declarations[0]
    facts.update(reason='native_clock_boundary_shapes', literal_schema_URL_agrees=article.get('url') == page_url,
                 declared_release=shape(article.get('datePublished')), declared_revision=shape(article.get('dateModified')))
    headline = article.get('headline')
    try:
        headline_hash = hashlib.sha256(headline.encode()).hexdigest() if isinstance(headline, str) else None
    except UnicodeEncodeError:
        headline_hash = None
    publisher = article.get('publisher')
    facts.update(schema_headline_sha256=headline_hash,
        schema_mainEntity_agrees=article.get('mainEntityOfPage') == {'@type': 'WebPage', '@id': page_url},
        schema_publisher_is_known_Organization=isinstance(publisher, dict)
            and publisher.get('@type') == 'Organization' and publisher.get('name') == '口袋學堂',
        declared_articleBody_supplied=bool(article.get('articleBody')))
    author = article.get('author')
    facts['declared_author_is_known_Organization'] = (isinstance(author, dict)
        and author.get('@type') == 'Organization' and author.get('name') == POCKET_AUTHOR
        and author.get('url') == POCKET_ORIGIN+'/school/')
    headings = [n for n in soup.select('main h1') if n.find_parent('article') is None]
    facts['page_heading_count'] = len(headings)
    if len(headings) == 1:
        header = headings[0].find_next_sibling()
        if header is not None:
            rows = header.select(':scope > div')
            facts['outer_label_count'] = len(rows)
            if len(rows) == 1:
                facts['outer_update_label'] = shape(rows[0].get_text(' ', strip=True))
    bodies = soup.select('article.cms-body > .cms-article-scope')
    facts['body_scope_count'] = len(bodies)
    if len(bodies) == 1:
        inner = bodies[0].select(':scope > article.pocket-article > .article-meta')
        facts['inner_label_count'] = len(inner)
        facts['inner_update_labels'] = [shape(n.get_text(' ', strip=True)) for n in inner[:2]]
    return facts


def pocket_identity_metadata(soup, *, page_url):
    """Finite native-boundary facts, never a source/scope or article grant.

    Projected ``.pocket-native-body`` exists only after a successful parse, so
    its absence in source HTML cannot diagnose a missing native body. Inspect
    the actual CMS boundary once and expose only counts, hashes and known-tag
    categories. No arbitrary titles, names, classes, URLs or prose leave it.
    """
    facts = {'contract_version': 1, 'accepted_as_article': False,
        'raw_names_or_body_retained': False, 'publisher_requests': 0,
        'scope': 'native_identity_boundaries_not_candidate_body',
        'clock_shapes': pocket_clock_metadata(soup, page_url=page_url)}

    def visibility(node):
        try:
            _native_visible(node)
        except NewsError:
            return False
        return True

    def text_hash(node):
        try:
            return hashlib.sha256(node.get_text(' ', strip=True).encode()).hexdigest()
        except UnicodeEncodeError:
            return None

    for key, selector, attribute in (
            ('canonical', 'head > link[rel=canonical]', 'href'),
            ('og_URL', 'head > meta[property="og:url"]', 'content')):
        nodes = soup.select(selector)
        facts[key] = {'count': len(nodes), 'literal_page_URL_agrees':
            len(nodes) == 1 and nodes[0].get(attribute) == page_url}
    headings = [n for n in soup.select('main h1') if n.find_parent('article') is None]
    facts['page_heading_count'] = len(headings)
    facts['page_heading_sha256'] = [text_hash(n) for n in headings[:4]]
    facts['page_heading_visible'] = [visibility(n) for n in headings[:4]]
    facts['heading_schema_headline_hash_agrees'] = (len(headings) == 1
        and facts['page_heading_sha256'][0] is not None
        and facts['page_heading_sha256'][0] == facts['clock_shapes'].get('schema_headline_sha256'))
    native = soup.select('article.cms-body > .cms-article-scope')
    facts['native_scope_count'] = len(native)
    if len(headings) == 1:
        scope = headings[0].parent
        scoped = scope.select(':scope > article.cms-body > .cms-article-scope')
        facts['native_scope_under_heading_parent_count'] = len(scoped)
        headers = headings[0].find_next_sibling()
        credits = (headers.select(':scope > div > span.font-bold.text-slate-600.mr-2')
                   if headers is not None and headers.name == 'div' else [])
        facts['native_credit'] = {'count': len(credits), 'known_organisation_visible':
            len(credits) == 1 and credits[0].get_text(' ', strip=True) == POCKET_AUTHOR and visibility(credits[0])}
        og = soup.select('head > meta[property="og:title"]')
        facts['heading_literal_og_title_agrees'] = (len(og) == 1
            and og[0].get('content') == headings[0].get_text(' ', strip=True))
    if len(native) == 1:
        root = native[0]
        nested = root.select(':scope > article.pocket-article')
        facts['native_scope_visible'] = visibility(root)
        facts['nested_article_count'] = len(nested)
        facts['other_direct_element_count'] = sum(n.name != 'style' and all(n is not child for child in nested)
            for n in root.find_all(recursive=False))
        if len(nested) == 1:
            root = nested[0]
        facts['body_boundary'] = {
            'visible': visibility(root), 'text_characters': len(root.get_text(' ', strip=True)),
            'heading_count': len(root.select('h1')), 'meta_count': len(root.select(':scope > .article-meta')),
            'toc_count': len(root.select('nav')), 'cta_count': len(root.select(':scope > #section-cta')),
            'tag_counts': {tag: len(root.select(tag)) for tag in
                ('p','div','section','article','style','font','figure','figcaption','table','caption','iframe','script','img')},
            'unsupported_tag_count': sum(n.name not in _BODY_TAGS for n in root.find_all(True))}
    return facts


def _separate_brokerage_cta(native, notices, version):
    """Keep the witnessed brokerage CTA separate in nested and flat CMS prose."""
    ctas = native.select(':scope > #section-cta' if version >= 2 else ':scope > h2#section-cta')
    if not ctas:
        return
    tail = list(ctas[0].find_next_siblings())
    ad_index = 0
    if version >= 8:
        # CMS whitespace paragraphs are not an ad or intervening prose. Leave
        # them in the native body; ignore only visible text-only spacers when
        # finding the adjacent identified ad. Images/links/hidden nodes cannot
        # supply this boundary, and actual intervening prose is not skipped.
        while ad_index < len(tail):
            node = tail[ad_index]
            if (node.name != 'p' or node.get_text(' ', strip=True)
                    or any(child.name not in {'br', 'span', 'b', 'strong', 'em', 'i', 'u', 's'}
                           for child in node.find_all(True))):
                break
            for child in (node, *node.find_all(True)):
                _native_visible(child)
            ad_index += 1
    ad = tail[ad_index] if ad_index < len(tail) else None
    # The adjacent ad's identity, not its being last, defines its boundary.
    # v7 retains later native prose (including risk statements) for the usual
    # visibility/tag checks below; never erase everything following the CTA.
    boundary_ok = bool(ad is not None and (version >= 7 or len(tail) == 1))
    own_openaccount = bool(version >= 2 and boundary_ok
        and any(_own_openaccount_link(a.get('href', '')) for a in ad.select('a[href]')))
    branded = bool(boundary_ok and '口袋證券' in ad.get_text(' ', strip=True))
    own_action = []
    if version >= 6 and not branded and boundary_ok:
        action_labels = {'線上申請證券戶'}
        if version >= 8:
            action_labels.update({'線上 5 分鐘快速申請', '👉 線上 5 分鐘快速申請'})
        if version >= 9:
            action_labels.add('線上5分鐘快速申請')
        own_action = [a for a in ad.select('a[href]')
            if _own_openaccount_link(a.get('href', ''))
            and a.get_text(' ', strip=True) in action_labels]
        if len(own_action) == 1:
            for node in (ad, own_action[0], *own_action[0].find_all(True)):
                _native_visible(node)
    heading_ok = ctas[0].get_text(' ', strip=True).startswith('怎麼買' if version >= 2 else '怎麼買 ')
    if version >= 9:
        heading_ok = heading_ok or ctas[0].get_text(' ', strip=True) == '掌握富時指數調整，在口袋證券自行追蹤台股'
    if (len(ctas) != 1 or not boundary_ok or ad.name != 'div'
            or ctas[0].name not in ({'h2', 'p'} if version >= 2 else {'h2'})
            or not heading_ok
            or not (ad.select_one('a[href="https://pse.im/78wp56"]') is not None or own_openaccount)
            or not (branded or version >= 6 and len(own_action) == 1)):
        raise NewsError('article_identity_not_verified')
    following_count = len(tail) - ad_index - 1
    if following_count > 0 or ad_index:
        if len(native.select('#section-cta')) != 1:
            raise NewsError('article_identity_not_verified')
        for node in (ctas[0], *ctas[0].find_all(True), ad, *ad.find_all(True)):
            _native_visible(node)
    if ad_index:
        notices.append({'kind': 'native_whitespace_paragraphs_between_cta_and_ad_preserved',
            'paragraph_count': ad_index, 'whitespace_not_deleted_or_interpreted_as_prose': True})
    if following_count > 0:
        notices.append({'kind': 'native_brokerage_cta_has_following_prose_preserved',
            'following_native_sibling_count': following_count,
            'following_prose_not_removed': True})
    if not branded:
        notices.append({'kind': 'native_own_account_action_identifies_brokerage_cta',
            'visible_action': own_action[0].get_text(' ', strip=True),
            'source_url_raw': own_action[0]['href'], 'broker_name_text_invented': False,
            'account_form_or_media_requested': False})
    notices.append({'kind': 'separate_publisher_brokerage_cta_not_report_body',
        'text': ctas[0].get_text(' ', strip=True) + '\n' + _plain_text_node(ad)})
    ctas[0].decompose()
    ad.decompose()


def _separate_zero_layout_summary_label(native, notices):
    """One witnessed nonvisible label, never hidden article text or a gate."""
    marker = native.find(recursive=False)
    style = 'font-size:0px; line-height:0; height:0; margin:0; padding:0; overflow:hidden; visibility:hidden;'
    if (marker is None or marker.name != 'p' or marker.attrs != {'style': style}
            or marker.find(True) is not None or marker.get_text(' ', strip=True) != '重點速覽'):
        return
    following = marker.find_next_sibling()
    if following is None or following.name != 'ol':
        raise NewsError('article_identity_not_verified')
    items = following.find_all('li', recursive=False)
    if (not 1 <= len(items) <= 64
            or any(len(item.find_all('h3', recursive=False)) != 1
                   or not item.find_all('p', recursive=False) for item in items)):
        raise NewsError('article_identity_not_verified')
    _native_visible(native)
    _native_visible(following)
    notices.append({'kind': 'native_nonvisible_zero_layout_summary_label_not_body',
        'text_raw': marker.get_text(), 'style_raw': marker['style'],
        'visible_prose': False, 'body_text_inferred': False})
    marker.decompose()


def _separate_inline_update_label(native, notices):
    """One native update label with an ARIA-decorative separator, not prose.

    Do not make hidden characters generally visible. All three literal spans,
    the known date/organisation grammar and the exact decorative glyph must
    agree; every surrounding span/ancestor retains the normal visibility gate.
    """
    style = 'color: rgb(122, 128, 135); font-size: 14px;'
    for paragraph in native.find_all('p', recursive=False):
        spans = paragraph.find_all(recursive=False)
        if (paragraph.attrs or len(spans) != 3 or any(n.name != 'span' for n in spans)
                or spans[1].attrs != {'aria-hidden': 'true', 'style': style}
                or spans[1].find(True) is not None or spans[1].get_text() != '｜'
                or any(n.attrs != {'style': style} or n.find(True) is not None
                       for n in (spans[0], spans[2]))
                or any(str(n).strip() for n in paragraph.contents if n not in spans)):
            continue
        label = paragraph.get_text(' ', strip=True)
        try:
            clock = _inner_calendar_notice(label, datetime(2000, 1, 1).date(), 9)
        except NewsError:
            continue
        if clock.get('clock_role') != 'update_date':
            continue
        for node in (paragraph, spans[0], spans[2]):
            _native_visible(node)
        notices.append({'kind': 'native_inline_update_label_with_decorative_separator_not_body',
            'text_raw': label, 'calendar_date': clock['calendar_date'],
            'decorative_separator_aria_hidden': True, 'hidden_prose_made_visible': False,
            'label_not_promoted_to_release_or_revision_clock': True})
        paragraph.decompose()


def _reviewed_native_containers(native, version=9):
    """Keep witnessed summaries and written figure captions, without media IO."""
    accepted = set()
    for node in native.find_all(['aside', 'figure', 'figcaption']):
        if node.name == 'aside':
            children = node.find_all(recursive=False)
            if (node.parent is not native
                    or node.attrs != {'aria-label': '重點速覽', 'class': ['key-takeaways']}
                    or [child.name for child in children] != ['strong', 'ul']):
                raise NewsError('article_identity_not_verified')
            items = children[1].find_all('li', recursive=False)
            if not 1 <= len(items) <= 64 or children[0].get_text(' ', strip=True) != f'{len(items)}個重點速覽':
                raise NewsError('article_identity_not_verified')
            accepted.add(id(node))
        elif node.name == 'figure':
            children = node.find_all(recursive=False)
            linked = False
            if version >= 12 and [child.name for child in children] == ['a', 'figcaption']:
                anchor = children[0]
                images = anchor.find_all(recursive=False)
                try:
                    target = urlsplit(anchor.get('href', ''))
                except (TypeError, ValueError):
                    raise NewsError('article_identity_not_verified') from None
                linked = (set(anchor.attrs) <= {'href', 'target', 'rel', 'aria-label'}
                    and anchor.get('target') == '_blank' and anchor.get('rel') == ['noopener', 'noreferrer']
                    and not (target.scheme or target.netloc or target.query or target.fragment)
                    and bool(target.path) and '/' not in target.path and target.path.endswith('.png')
                    and [child.name for child in images] == ['img']
                    and not any(str(n).strip() for n in anchor.contents if n not in images)
                    and ('aria-label' not in anchor.attrs or isinstance(anchor['aria-label'], str)
                         and 0 < len(anchor['aria-label']) <= 256))
            allowed_attrs = {'style', 'id'} if version >= 12 else {'style'}
            identifier = node.get('id')
            if (node.parent is not native or set(node.attrs) - allowed_attrs
                    or (identifier is not None and (not isinstance(identifier, str)
                        or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,127}', identifier)
                        or len(native.find_all(id=identifier)) != 1))
                    or ([child.name for child in children] != ['img', 'figcaption'] and not linked)
                    or any(str(n).strip() for n in node.contents if n not in children)):
                raise NewsError('article_identity_not_verified')
            accepted.update((id(node), id(children[1])))
        elif id(node) not in accepted:
            raise NewsError('article_identity_not_verified')
    return accepted


def _separate_native_nonprose(native, notices):
    """Never execute CSS/JSON or fetch an embed; preserve received originals."""
    def dom_text_hash(text):
        # HTML tokenization normalizes source CRLF/CR to LF. Hash this named
        # DOM representation, never mislabel it as the retained raw-byte hash.
        return hashlib.sha256(text.replace('\r\n', '\n').replace('\r', '\n').encode()).hexdigest()

    for node in list(native.select('style, script, iframe')):
        if node.name == 'iframe':
            _native_visible(node)
            src = node.get('src', '')
            try:
                parts = urlsplit(src)
                pairs = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True, max_num_fields=1)
                query_ok = not pairs or len(pairs) == 1 and pairs[0][0] == 'si' and bool(re.fullmatch(r'[A-Za-z0-9_-]{1,128}', pairs[0][1]))
            except (ValueError, TypeError):
                raise NewsError('article_identity_not_verified') from None
            allowed = {'src', 'title', 'width', 'height', 'frameborder', 'allow', 'allowfullscreen', 'referrerpolicy', 'loading'}
            permissions = {'accelerometer', 'autoplay', 'clipboard-write', 'encrypted-media', 'gyroscope', 'picture-in-picture', 'web-share'}
            if (set(node.attrs) - allowed or parts.scheme != 'https' or parts.netloc != 'www.youtube.com'
                    or not re.fullmatch(r'/embed/[A-Za-z0-9_-]{11}', parts.path) or parts.fragment or not query_ok
                    or node.get('title') != 'YouTube video player' or node.find(True) is not None or node.get_text(strip=True)
                    or node.get('frameborder', '0') != '0' or node.get('allowfullscreen', '') != ''
                    or node.get('referrerpolicy', 'strict-origin-when-cross-origin') != 'strict-origin-when-cross-origin'
                    or node.get('loading', 'lazy') not in {'lazy', 'eager'}
                    or any(not re.fullmatch(r'[0-9]{1,4}', node.get(k, '1')) or not 1 <= int(node.get(k, '1')) <= 4096 for k in ('width', 'height'))
                    or any(p.strip() not in permissions for p in node.get('allow', '').split(';') if p.strip())):
                raise NewsError('article_identity_not_verified')
            notices.append({'kind': 'unfetched_native_YouTube_embed_reference_not_transcribed',
                'source_url_raw': src, 'title_raw': node['title'], 'media_requested': False,
                'transcript_requested': False, 'media_or_transcript_text_inferred': False})
        elif node.name == 'style':
            notices.append({'kind': 'native_CSS_not_written_prose_not_executed',
                'DOM_text_LF_sha256': dom_text_hash(node.get_text()),
                'hash_basis': 'HTML_input_newlines_CRLF_or_CR_to_LF', 'executed': False})
        else:
            # Only the witnessed FAQ metadata duplicate, not arbitrary JS or
            # another embedded article, may be separated from written prose.
            if node.attrs != {'type': 'application/ld+json'}:
                raise NewsError('article_identity_not_verified')
            try:
                raw = node.string if node.string is not None else node.get_text()
                if len(raw) > 128 << 10:
                    raise ValueError('unbounded FAQ declaration')
                value = json.loads(raw, object_pairs_hook=_unique_object)
                roots = value if isinstance(value, list) else [value]
                if len(roots) != 1 or not isinstance(roots[0], dict) or roots[0].get('@type') != 'FAQPage':
                    raise ValueError('FAQPage only')
                entries = roots[0].get('mainEntity')
                if (not isinstance(entries, list) or not 1 <= len(entries) <= 64
                        or any(not isinstance(q, dict) or q.get('@type') != 'Question' or not isinstance(q.get('name'), str)
                            or not isinstance(q.get('acceptedAnswer'), dict) or q['acceptedAnswer'].get('@type') != 'Answer'
                            or not isinstance(q['acceptedAnswer'].get('text'), str) for q in entries)):
                    raise ValueError('bounded FAQ questions required')
            except (ValueError, TypeError, RecursionError):
                raise NewsError('article_identity_not_verified') from None
            notices.append({'kind': 'native_FAQ_JSONLD_not_a_second_written_body',
                'DOM_text_LF_sha256': dom_text_hash(raw), 'hash_basis': 'HTML_input_newlines_CRLF_or_CR_to_LF',
                'questions': len(entries),
                'executed': False, 'body_text_inferred_from_JSON': False})
        node.decompose()


def pocket_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Verify the actual final page and native author before projecting text."""
    if pocket_article_id(page_url, adapter_contract_version=adapter_contract_version) is None:
        raise NewsError('article_identity_not_verified')
    if adapter_contract_version not in {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}:
        raise NewsError('article_identity_not_verified')
    if adapter_contract_version == 1:
        heading = _one(soup, 'main h1')
    else:
        # The CMS sometimes repeats the same title *inside* its native body.
        # It is not a second page identity. Other competing page headers still
        # fail; the inner title must independently match below.
        headings = [node for node in _one(soup, 'main').select('h1')
                    if node.find_parent('article') is None]
        if len(headings) != 1:
            raise NewsError('article_identity_not_verified')
        heading = headings[0]
    scope = heading.parent
    title = heading.get_text(' ', strip=True)
    if (not title or _one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'
            or _one(soup, 'head > meta[property="og:title"]').get('content') != title):
        raise NewsError('article_identity_not_verified')
    declarations = []
    scripts = soup.select('script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 16:
        raise NewsError('article_identity_not_verified')
    for script in scripts:
        try:
            text = script.get_text()
            if len(text) > 128 << 10:
                raise ValueError('unbounded native declaration')
            objects = _objects(json.loads(text, object_pairs_hook=_unique_object))
            declarations.extend(obj for obj in objects if obj.get('@type') == 'Article')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(declarations) != 1:
        raise NewsError('article_identity_not_verified')
    article = declarations[0]
    publisher = article.get('publisher')
    if (article.get('headline') != title or article.get('url') != page_url
            or article.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
            or publisher.get('name') != '口袋學堂' or article.get('articleBody')):
        raise NewsError('article_identity_not_verified')
    if str(article.get('isAccessibleForFree', 'true')).lower() == 'false':
        raise NewsError('paywall_or_login')
    header = heading.find_next_sibling()
    if header is None or header.name != 'div':
        raise NewsError('article_attribution_not_verified')
    byline = _one(header, ':scope > div > span.font-bold.text-slate-600.mr-2',
                  'article_attribution_not_verified')
    author = article.get('author')
    if (byline.get_text(' ', strip=True) != POCKET_AUTHOR or not isinstance(author, dict)
            or author.get('@type') != 'Organization' or author.get('name') != POCKET_AUTHOR
            or author.get('url') != POCKET_ORIGIN + '/school/'):
        raise NewsError('article_rights_or_scope_excluded')
    label = _one(header, ':scope > div').get_text(' ', strip=True)
    match = re.fullmatch(POCKET_AUTHOR + r' 市場分析師 更新於 ([0-9]{4}/[0-9]{2}/[0-9]{2})', label)
    published, modified = article.get('datePublished'), article.get('dateModified')
    try:
        if (match is None or not isinstance(published, str) or not isinstance(modified, str)
                or not _CLOCK.fullmatch(published) or not _CLOCK.fullmatch(modified)):
            raise ValueError('unverified source clock declaration')
        visible_day = datetime.strptime(match[1], '%Y/%m/%d').date()
        release = datetime.fromisoformat(published.replace('Z', '+00:00'))
        revision = datetime.fromisoformat(modified.replace('Z', '+00:00'))
        if revision < release and adapter_contract_version == 1:
            raise ValueError('revision precedes release')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    native = _one(scope, ':scope > article.cms-body > .cms-article-scope')
    for node in (heading, header, byline, native):
        _native_visible(node)
    notices = [{'kind': 'native_visible_update_label', 'text': label,
        'visible_date_raw': match[1], 'visible_precision': 'date_only',
        'matches_machine_release_Taipei_day': visible_day == release.astimezone(timezone(timedelta(hours=8))).date(),
        'matches_machine_revision_Taipei_day': visible_day == revision.astimezone(timezone(timedelta(hours=8))).date(),
        'label_is_not_promoted_to_release_or_revision_clock': True}]
    if revision < release:
        notices.append({'kind': 'source_modification_precedes_publication',
            'declared_publication': published, 'declared_modification': modified,
            'availability_backdated_to_modification': False, 'source_reason_inferred': False})
    retained_inner_heading = None
    nested = native.select(':scope > article.pocket-article')
    if nested:
        if (len(nested) != 1 or any(node.name != 'style' and node is not nested[0]
                                   for node in native.find_all(recursive=False))):
            raise NewsError('article_identity_not_verified')
        native = nested[0]
        _native_visible(native)
        metas = native.select(':scope > .article-meta')
        prose_metas = False
        if adapter_contract_version >= 9 and metas:
            # A CMS class is not a clock. Preserve visible context/disclaimer
            # paragraphs in their exact native position; do not force them
            # into one update date or discard their written values. A single
            # previously accepted calendar label keeps its existing output.
            try:
                if len(metas) != 1:
                    raise NewsError('invalid_bulk_publication_clock')
                _inner_calendar_notice(metas[0].get_text(' ', strip=True), visible_day, adapter_contract_version)
            except NewsError:
                prose_metas = (1 <= len(metas) <= 8 and all(n.name == 'p'
                    and n.attrs == {'class': ['article-meta']} and n.get_text(' ', strip=True) for n in metas))
                if prose_metas:
                    notices.append({'kind': 'native_article_meta_paragraphs_preserved_as_body',
                        'paragraph_count': len(metas), 'calendar_or_credit_inferred_from_class': False,
                        'not_promoted_to_release_or_revision_clock': True})
        if (metas and not prose_metas) or adapter_contract_version == 1:
            meta = _one(native, ':scope > .article-meta')
            meta_label = meta.get_text(' ', strip=True)
            if (adapter_contract_version >= 3
                    and meta_label != f'更新於 {visible_day.isoformat()} ｜ {POCKET_AUTHOR}'):
                # New calendar forms require visible native evidence. Keep
                # the already accepted v2 repeated-label output unchanged;
                # it never supplied the publication/revision clocks either.
                for node in (meta, *meta.find_all(True)):
                    _native_visible(node)
            notices.append(_inner_calendar_notice(meta_label, visible_day, adapter_contract_version))
            meta.decompose()
        if adapter_contract_version >= 2:
            repeated_titles = native.select('h1')
            if repeated_titles:
                if len(repeated_titles) != 1 or repeated_titles[0].parent is not native:
                    raise NewsError('article_identity_not_verified')
                inner = repeated_titles[0]
                _native_visible(inner)
                inner_title = inner.get_text(' ', strip=True)
                if inner_title == title:
                    notices.append({'kind': 'native_repeated_title_not_second_body', 'text': title})
                    inner.decompose()
                elif adapter_contract_version >= 5 and inner_title:
                    # A source's native body heading need not be its outer
                    # headline. Identity is independently verified above.
                    # Keep the distinct heading in its actual body position;
                    # never discard/rewrite it or replace the declared title.
                    retained_inner_heading = inner
                    notices.append({'kind': 'native_distinct_inner_heading_preserved_in_body',
                        'text': inner_title, 'outer_headline_replaced': False,
                        'heading_used_as_alternate_article_identity': False})
                else:
                    raise NewsError('article_identity_not_verified')
            for toc in native.select('nav'):
                _native_visible(toc)
                links = toc.select('a')
                allowed_toc = (['toc'], ['toc-box']) if adapter_contract_version >= 9 else (['toc'],)
                if (toc.parent is not native or toc.get('class') not in allowed_toc
                        or toc.get('aria-label') != '文章目錄' or not links
                        or any(node.name not in {'strong', 'ol', 'ul', 'li', 'a'}
                               for node in toc.find_all(True))):
                    raise NewsError('article_identity_not_verified')
                for link in links:
                    target = link.get('href', '')
                    matches = native.find_all(id=target[1:]) if target.startswith('#') else []
                    if (len(matches) != 1 or matches[0].name not in {'h2', 'h3', 'section'}
                            or matches[0] in toc.descendants):
                        raise NewsError('article_identity_not_verified')
                for node in toc.find_all(True):
                    _native_visible(node)
                notices.append({'kind': 'native_internal_table_of_contents_not_second_body',
                                'text': _plain_text_node(toc)})
                toc.decompose()
        if adapter_contract_version >= 4:
            _separate_native_nonprose(native, notices)
        _separate_brokerage_cta(native, notices, adapter_contract_version)
    elif native.find('article') is not None and adapter_contract_version < 4:
        raise NewsError('article_identity_not_verified')
    if not nested and adapter_contract_version >= 4:
        _separate_native_nonprose(native, notices)
        _separate_brokerage_cta(native, notices, adapter_contract_version)
    if adapter_contract_version >= 8:
        _separate_zero_layout_summary_label(native, notices)
    reviewed_containers = set()
    if adapter_contract_version >= 9:
        _separate_inline_update_label(native, notices)
        reviewed_containers = _reviewed_native_containers(native, adapter_contract_version)
    for node in native.find_all(True):
        nonvisible_caption = (adapter_contract_version >= 4 and node.name == 'caption'
            and node.attrs == {'style': 'display:none;'} and node.parent.name == 'table'
            and len(node.parent.find_all('caption', recursive=False)) == 1
            and node.find(True) is None and 0 < len(node.get_text(' ', strip=True)) <= 256)
        if nonvisible_caption:
            # Exact plain table context, not a hidden alternative article.
            # Check every ancestor normally; never erase/override its styling
            # or claim this caption was visible publication evidence.
            _native_visible(node.parent, accessible_table_captions=True)
            notices.append({'kind': 'native_nonvisible_plain_table_caption_retained_as_context',
                'text': node.get_text(' ', strip=True), 'style_raw': node['style'],
                'visible_prose': False, 'not_an_invisible_alternative_article': True})
        else:
            _native_visible(node, accessible_table_captions=True)
        if (node.name not in _BODY_TAGS
                and not (adapter_contract_version >= 4 and node.name == 'article')
                and id(node) not in reviewed_containers
                and node is not retained_inner_heading):
            raise NewsError('article_identity_not_verified')
        if node.name == 'img':
            notices.append({'kind': 'unfetched_inline_image_reference_not_text_or_licensed_media',
                'source_url_raw': node.get('src'), 'alt_raw': node.get('alt'),
                'media_requested': False, 'image_data_or_text_inferred': False})
        if node.name == 'caption' and node.get('class') == ['sr-only']:
            notices.append({'kind': 'native_accessible_table_caption_preserved_in_table_text',
                'text': node.get_text(' ', strip=True), 'not_an_invisible_alternative_article': True})
    first = native.find(['p', 'h2', 'h3'])
    if first and re.match(r'^(?:轉載|轉自|本文來源|新聞來源|來源[：:])', first.get_text(' ', strip=True)):
        raise NewsError('article_rights_or_scope_excluded')
    for node in native.find_all(['aside', 'figure']) if reviewed_containers else ():
        # The common text walker correctly excludes unrelated page asides and
        # figures. Only these already-verified *body* containers are projected
        # as neutral blocks, or their native summary/caption would disappear.
        # Do not relax the shared exclusion or fetch/infer image text.
        assert id(node) in reviewed_containers
        notices.append({'kind': 'native_verified_body_container_prose_preserved',
            'source_tag': node.name, 'projected_tag': 'div', 'source_layout_retained_in_original': True,
            'media_requested': False, 'media_text_inferred': False})
        node.name = 'div'
    # Move, do not reparse or concatenate nested matches. Ordered text/table
    # cells retain their exact native values; immutable raw retains layout.
    normalized = soup.new_tag('div', attrs={'class': 'pocket-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    body_scope = soup.new_tag('div')
    body_scope.append(normalized)
    return {
        'headline': title, 'author': [{'name': POCKET_AUTHOR}],
        'author_attribution_detail_json': json.dumps({'native_credit': POCKET_AUTHOR,
            'native_organization_credit_not_inferred_named_reporter': True,
            'schema_author_url': author['url']}, ensure_ascii=False),
        'datePublished': published, 'dateModified': modified,
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': 'publisher_current_schema_UTC_release_and_revision_exact_source_precision; '
            'visible_date_label_says_updated_not_publication_and_is_preserved_separately; '
            'not_observed_historical_PIT',
        'article_identity_basis': 'observed_final_Pocket_URL_canonical_OG_single_Article_schema_heading_and_native_editorial_credit_v1',
        'body_completeness': ('current_native_editorial_prose_tables_FAQ_once; '
            'brokerage_CTA_separate; images_unfetched_not_OCRed; not_original_historical_version'
            + ('; embedded_YouTube_reference_unfetched_not_transcribed' if any(
                n['kind'] == 'unfetched_native_YouTube_embed_reference_not_transcribed' for n in notices) else '')),
    }, body_scope
