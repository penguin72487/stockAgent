"""StockFeel's declared original and visible prose; no HTTP or script execution.

The source initially hides its two header credits pending client rendering.
They are retained as declarations, not verified rendered bylines or people.
An article about an old quarter is not evidence of publication in that quarter.
"""
from __future__ import annotations

from datetime import datetime
from collections import Counter
import hashlib
import json
import re
import unicodedata
from urllib.parse import unquote, urlsplit

from bs4 import NavigableString

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


STOCKFEEL_ORIGIN = 'https://www.stockfeel.com.tw'
STOCKFEEL_LAYOUT_VERSIONS = {
    'stockfeel_visible_original_v1': 1, 'stockfeel_visible_original_v2': 2,
    'stockfeel_visible_original_v3': 3,
    'stockfeel_visible_original_v4': 4,
    'stockfeel_visible_original_v5': 5,
    'stockfeel_visible_original_v6': 6,
    'stockfeel_visible_original_v7': 7,
}
# Full old capture, new capture/auth, and original HTTP identities are pinned
# only after the reviewed native declarations and unchanged transport policy.
STOCKFEEL_V2_HTTP_BINDING = (
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '69fd82d808eb27163fb1962acb171ceacb525dda8a3af7274e606c5bff3ae670',
    'ceb68367200163247d38d1db0bb96fee0a8f7958c627491b924d7822c2081476',
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '8b77f257957ac1c594e272b67f5eb8c32efbfdfa76c10e6cc7befd76b0429b70',
)
STOCKFEEL_V2_REPAIR_COHORT = frozenset({
    STOCKFEEL_ORIGIN + '/%E5%9C%8B%E7%A5%A8%E9%87%91-2889-2026q3-%E6%B3%95%E8%AA%AA%E6%9C%83/',
})
STOCKFEEL_V2_PRIOR_SCOPE_CUTOFF = '2026-10-21T21:01:05+00:00'
STOCKFEEL_V3_HTTP_BINDING = (
    '69fd82d808eb27163fb1962acb171ceacb525dda8a3af7274e606c5bff3ae670',
    '1a214591af69ba2b01e343f56188a7633cf5694837c825a8115545159c8df356',
    'ccec5553aed418ec9b5c61b4bd7f9a450b398c3ebb1576a3b1e0c85a1a3f9ed5',
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '8b77f257957ac1c594e272b67f5eb8c32efbfdfa76c10e6cc7befd76b0429b70',
)
STOCKFEEL_V3_PRIOR_SCOPE_CUTOFF = '2026-10-21T21:24:21.026213+00:00'
STOCKFEEL_V4_HTTP_BINDING = (
    '1a214591af69ba2b01e343f56188a7633cf5694837c825a8115545159c8df356',
    '868e6c1e34c326538f924d48ca1f437ed6aad9406c19a3bd8a87266692e79151',
    'ccec5553aed418ec9b5c61b4bd7f9a450b398c3ebb1576a3b1e0c85a1a3f9ed5',
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '8b77f257957ac1c594e272b67f5eb8c32efbfdfa76c10e6cc7befd76b0429b70',
)
STOCKFEEL_V5_HTTP_BINDING = (
    '868e6c1e34c326538f924d48ca1f437ed6aad9406c19a3bd8a87266692e79151',
    '9c276761db5cc481062631dc6feb63d7d22cda1eaeb5162e8205ac19ec401b52',
    'ccec5553aed418ec9b5c61b4bd7f9a450b398c3ebb1576a3b1e0c85a1a3f9ed5',
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '8b77f257957ac1c594e272b67f5eb8c32efbfdfa76c10e6cc7befd76b0429b70',
)
STOCKFEEL_V6_HTTP_BINDING = (
    '9c276761db5cc481062631dc6feb63d7d22cda1eaeb5162e8205ac19ec401b52',
    '6a336a09ca3cc0f8285eb75f56e1bc1010cba1222bc19f7981f383fbca22f823',
    'ccec5553aed418ec9b5c61b4bd7f9a450b398c3ebb1576a3b1e0c85a1a3f9ed5',
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '8b77f257957ac1c594e272b67f5eb8c32efbfdfa76c10e6cc7befd76b0429b70',
)
STOCKFEEL_V4_CREDIT_NOTE = 'Author name, numeric ID, same-origin literal profile, avatar and head declaration must agree independently of exact source67/stockfeel.'
STOCKFEEL_V5_CREDIT_NOTE = 'Native writer name, numeric ID, same-origin literal profile and avatar must agree independently of exact source67/stockfeel. Additional head coauthors or a distinct declared StockFeel group remain separate source roles, not verified people or equivalent identities. v5 recognizes only empty wbr tags and the bounded JSON-only TradingView widget pair; the interactive chart is not requested, executed or captured as article text.'
STOCKFEEL_V5_VERIFICATION = 'native_UTF8_independent_credit_roles_empty_wbr_closed_chart_pair_v5_scoped_20261022'
STOCKFEEL_V6_CREDIT_NOTE = STOCKFEEL_V5_CREDIT_NOTE + ' v6 compares only ASCII title whitespace and the witnessed spaced en-dash after a parenthesized numeric stock code with a hyphen. Literal native, document and OpenGraph title declarations remain separate and unchanged; no fuzzy title, numeric-sign or canonical-alias inference.'
STOCKFEEL_V6_VERIFICATION = 'native_UTF8_title_whitespace_stock_code_separator_v6_scoped_20261022'
STOCKFEEL_V7_HTTP_BINDING = (
    '6a336a09ca3cc0f8285eb75f56e1bc1010cba1222bc19f7981f383fbca22f823',
    '91268f315f5df18ad331002ca50b43a8d80b05aa05e3a49e5de7608205574d67',
    'ccec5553aed418ec9b5c61b4bd7f9a450b398c3ebb1576a3b1e0c85a1a3f9ed5',
    '487da0f640dd2daba61472d15b82fe7d07f4cc1a532988d6b03231a5ea72bb36',
    '8b77f257957ac1c594e272b67f5eb8c32efbfdfa76c10e6cc7befd76b0429b70',
)
STOCKFEEL_V7_CREDIT_NOTE = 'Linked native writer name, numeric ID, literal same-origin profile and avatar remain independently checked beside exact source67/stockfeel. v7 also retains a bounded plain-text native writer with explicitly empty numeric ID and no profile, only when head publisher group, publisher avatar and exact article-source67/stockfeel independently agree. Missing writer ID/profile remain missing; no guessed link, verified person or ownership. A distinct native writer and exact head publisher group stay separate roles, not equivalent names. The v5 coauthor/group, empty wbr and closed unrequested TradingView rules and v6 literal title comparison remain unchanged.'
STOCKFEEL_V7_VERIFICATION = 'native_UTF8_named_unlinked_writer_and_distinct_head_publisher_roles_v7_scoped_20261022'
STOCKFEEL_PATH_PATTERN = r'^/(?:[A-Za-z0-9_-]|%[0-9A-Fa-f]{2})+/$'
STOCKFEEL_BODY_SELECTOR = ':scope > article.sin-content-body > .sin-content > .sin-content-original'
STOCKFEEL_BOUNDARY_QUESTION = 'stockfeel_native_credit_and_written_boundary_v1'
_AUTHOR = '股感時事特派員'
_PROFILE = STOCKFEEL_ORIGIN + '/author/news'
_COPYRIGHT = ('© Stockfeel. All rights reserved '
              '股感服務之軟體開發、營運及作業環境通過 ISO/IEC 27001:2022 驗證')
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
_BODY_TAGS = frozenset({'p', 'br', 'img', 'figure', 'figcaption', 'h2', 'h3', 'h4', 'h5', 'h6',
    'div', 'span', 'a', 'strong', 'b', 'em', 'i', 'u', 's', 'del', 'sub', 'sup', 'ul', 'ol',
    'li', 'blockquote', 'pre', 'code', 'hr', 'table', 'caption', 'colgroup', 'col', 'thead',
    'tbody', 'tfoot', 'tr', 'th', 'td'})
_HEAD_GROUP_DECLARATIONS = frozenset({'股感商品研究室', '股感產業研究員', '股感時事特派員'})


def _interactive_chart_notices(native):
    """Recognize a bounded literal widget pair, never evaluate or request it.

    Only the witnessed loader and a JSON-only TradingView constructor are
    non-written content. A matching empty mount and literal provider credit
    may precede the pair; the credit stays in written text, never discarded.
    Unknown scripts, executable expressions and other container text fail.
    """
    scripts = native.find_all('script')
    if not scripts:
        return []
    if len(scripts) > 16:
        raise NewsError('article_identity_not_verified')

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate chart key')
            value[key] = item
        return value

    notices, covered = [], set()
    for script in scripts:
        if id(script) in covered:
            continue
        parent = script.parent
        children = parent.find_all(recursive=False)
        pair = children[-2:]
        if (parent.name != 'div' or parent.attrs != {'class': ['tradingview-widget-container']}
                or len(children) not in {2, 4} or any(node.name != 'script' for node in pair)
                or pair[0] is not script
                or any(str(node).strip() for node in parent.children if getattr(node, 'name', None) is None)):
            raise NewsError('article_identity_not_verified')
        loader, constructor = pair
        if (loader.attrs != {'type': 'text/javascript', 'src': 'https://s3.tradingview.com/tv.js'}
                or loader.get_text().strip() or constructor.attrs != {'type': 'text/javascript'}):
            raise NewsError('article_identity_not_verified')
        text = constructor.get_text()
        if len(text) > 4096:
            raise NewsError('article_identity_not_verified')
        match = re.fullmatch(r'\s*new TradingView[.]widget\(\s*(\{.*\})\s*\);\s*', text, re.S)
        if match is None:
            raise NewsError('article_identity_not_verified')
        try:
            config = json.loads(match[1], object_pairs_hook=unique_object)
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
        expected = {'width', 'height', 'symbol', 'interval', 'timezone', 'theme', 'style',
            'locale', 'toolbar_bg', 'enable_publishing', 'allow_symbol_change', 'container_id'}
        if (not isinstance(config, dict) or set(config) != expected
                or config['width'] != '100%' or type(config['height']) is not int or not 1 <= config['height'] <= 2048
                or not isinstance(config['symbol'], str) or re.fullmatch(r'[A-Z0-9:._-]{1,80}', config['symbol']) is None
                or config['interval'] not in ('1', '3', '5', '15', '30', '60', '120', 'D', 'W', 'M')
                or config['timezone'] != 'Asia/Taipei' or config['theme'] not in ('light', 'dark')
                or config['style'] != '1' or config['locale'] != 'zh_TW'
                or not isinstance(config['toolbar_bg'], str) or re.fullmatch(r'#[0-9a-fA-F]{6}', config['toolbar_bg']) is None
                or config['enable_publishing'] is not False or type(config['allow_symbol_change']) is not bool
                or not isinstance(config['container_id'], str)
                or re.fullmatch(r'tradingview_[A-Za-z0-9]{1,32}', config['container_id']) is None):
            raise NewsError('article_identity_not_verified')
        if len(children) == 4:
            mount, credit = children[:2]
            anchor = credit.find_all('a', recursive=False)
            span = anchor[0].find_all('span', recursive=False) if len(anchor) == 1 else []
            symbol_label = config['symbol'].rsplit(':', 1)[-1] + '圖表'
            if (mount.name != 'div' or mount.attrs != {'id': config['container_id']} or mount.contents
                    or credit.name != 'div' or credit.attrs != {'class': ['tradingview-widget-copyright']}
                    or len(anchor) != 1 or len(span) != 1 or len(credit.find_all()) != 2
                    or anchor[0].attrs != {'href': 'https://tw.tradingview.com/symbols/' + config['symbol'].replace(':', '-') + '/',
                        'rel': ['noopener'], 'target': '_blank'}
                    or span[0].attrs != {'class': ['blue-text']} or span[0].get_text() != symbol_label
                    or anchor[0].get_text() != symbol_label or credit.get_text() != symbol_label + '由TradingView提供'):
                raise NewsError('article_identity_not_verified')
        covered.update(map(id, pair))
        notices.append({'kind': 'unrequested_interactive_chart_configuration',
            'script_url': loader['src'], 'source_configuration_json': match[1],
            'static_widget_credit_retained_in_written_body': len(children) == 4,
            'script_executed_or_external_media_requested': False,
            'chart_values_in_written_article_body': False, 'not_a_historical_price_observation': True})
    return notices


def stockfeel_boundary_source(source):
    """The current known-URL source, not a new capture or rights candidate."""
    return (source.provider == 'stockfeel' and source.allowed and source.request_enabled
        and source.domains == ('www.stockfeel.com.tw',)
        and source.embedded_article_layout in STOCKFEEL_LAYOUT_VERSIONS
        and source.acquisition_mode == 'continuous'
        and source.known_url_only
        and source.authorization.get('kind') == 'personal_research')


def stockfeel_role_repair_source(source):
    """Only the pinned author/source separation, never a general rights retry."""
    return bool(stockfeel_boundary_source(source)
        and source.embedded_article_layout == 'stockfeel_visible_original_v2'
        and len(STOCKFEEL_V2_HTTP_BINDING) == 5
        and (source.data_fingerprint, source.authorization_fingerprint) == STOCKFEEL_V2_HTTP_BINDING[1:3])


def stockfeel_writer_repair_source(source):
    """The exact source67/native-writer contract, not arbitrary rights retries."""
    binding = {'stockfeel_visible_original_v3': STOCKFEEL_V3_HTTP_BINDING,
               'stockfeel_visible_original_v4': STOCKFEEL_V4_HTTP_BINDING}.get(source.embedded_article_layout, ())
    return bool(stockfeel_boundary_source(source) and len(binding) == 5
        and (source.data_fingerprint, source.authorization_fingerprint) == binding[1:3])


def stockfeel_writer_profile(value, *, adapter_contract_version=3):
    """Validate a literal native profile, including UTF-8 author slugs.

    Return the original spelling. Do not infer aliases, fetch the profile,
    expand an origin, or rewrite a previously closed v14 observation.
    """
    if type(adapter_contract_version) is not int or adapter_contract_version not in {3, 4}:
        raise ValueError('unreviewed_stockfeel_profile_contract')
    if (not isinstance(value, str) or len(value) > 2048 or '?' in value or '#' in value
            or any(not char.isprintable() or char.isspace() for char in value)):
        return None
    try:
        parts = urlsplit(value)
        if (parts.scheme != 'https' or parts.netloc != 'www.stockfeel.com.tw'
                or parts.query or parts.fragment
                or re.search(r'%(?:2f|5c|25)', parts.path, re.I)
                or re.search(r'%(?![0-9a-fA-F]{2})', parts.path)):
            return None
        path = unquote(parts.path, encoding='utf-8', errors='strict')
    except (UnicodeError, ValueError):
        return None
    match = re.fullmatch(r'/author/([^/]{1,128})/?', path)
    if match is None:
        return None
    punctuation = '_-.' if adapter_contract_version == 4 else '_-'
    if (adapter_contract_version == 4 and not any(char.isalnum() for char in match[1])
            or not all(char.isalnum() or char in punctuation or unicodedata.category(char).startswith('M')
                       for char in match[1])):
        return None
    return value


def stockfeel_boundary_metadata(soup, *, page_url):
    """One DOM's bounded declarations/counts, no prose, people or permission.

    Unknown credits stay hashes. Recognized public publisher-group categories and
    literal same-origin author paths are observations, never ownership proof.
    Unlike document-level :scope, body selection is relative to its wrapper.
    """
    def digest(value):
        return hashlib.sha256(value.encode()).hexdigest()

    def profile(value):
        return value if isinstance(value, str) and re.fullmatch(
            re.escape(STOCKFEEL_ORIGIN) + r'/author/[A-Za-z0-9_-]{1,128}/?', value) else None

    def named(value):
        return {'recognized_public_group': {
            '股感時事特派員': 'stockfeel_news_team', '股感產業研究員': 'stockfeel_industry_research',
            '股感商品研究室': 'stockfeel_product_research', 'StockFeel 股感': 'stockfeel_publisher_label'}.get(value),
            'characters': len(value), 'sha256': digest(value)}

    def declared_urls(selector, attribute):
        nodes = soup.select(selector)
        declarations = []
        for node in nodes[:2]:
            value = str(node.get(attribute, ''))
            key = stockfeel_url_key(value) if len(value) <= 2048 else None
            declarations.append({'literal_same_origin_article_url': value if key is not None else None,
                'sha256': digest(value), 'matches_requested_page_key': key is not None and key == stockfeel_url_key(page_url)})
        return {'node_count': len(nodes), 'declarations': declarations, 'truncated': len(nodes) > 2}

    canonical = declared_urls('head > link[rel=canonical]', 'href')
    og_urls = declared_urls('head > meta[property="og:url"]', 'content')
    scopes = soup.select('.single-content > .row > .col-md-12 > .sin-content-wrapper')
    headers = [header for scope in scopes[:2] for header in scope.select(':scope > .sin-content-header')[:2]]
    nodes = [node for header in headers for node in header.select(':scope > .flex > .flex-wrap > div[data-aid]')]
    credits = []
    for node in nodes[:8]:
        spans = node.find_all('span', recursive=False)
        label = spans[0].get_text(' ', strip=True) if spans else ''
        anchors = spans[1].select(':scope > a') if len(spans) == 2 else []
        link = anchors[0] if len(anchors) == 1 else None
        name = link.get_text(' ', strip=True) if link is not None else ''
        href = str(link.get('href', '')) if link is not None else ''
        aid = str(node.get('data-aid', ''))
        credits.append({'label': label if label in {'作者', '文章來源'} else None,
            'label_sha256': digest(label), 'data_aid': aid if re.fullmatch(r'[0-9]{1,8}', aid) else None,
            'data_aid_sha256': digest(aid), 'direct_span_count': len(spans),
            'direct_link_count': len(anchors), **named(name), 'literal_profile_url': profile(href),
            'profile_sha256': digest(href), 'initial_visibility_hidden': bool(re.search(
                r'visibility\s*:\s*hidden', str(node.get('style', '')), re.I)),
            'hidden_attribute': node.has_attr('hidden'), 'aria_hidden_attribute': node.has_attr('aria-hidden')})
    authors = soup.select('head > meta[name=author]')
    natives = [native for scope in scopes[:2] for native in scope.select(STOCKFEEL_BODY_SELECTOR)[:2]]
    tags = Counter(node.name for native in natives for node in native.find_all())
    notices = soup.select('footer .copyright > span')
    clocks = {}
    for field in ('published_time', 'modified_time'):
        values = soup.select(f'head > meta[property="article:{field}"]')
        clocks[field] = {'node_count': len(values), 'declarations': [
            {'seconds_and_offset_raw': str(node.get('content', '')) if _CLOCK.fullmatch(str(node.get('content', ''))) else None,
             'sha256': digest(str(node.get('content', '')))} for node in values[:2]]}
    return {'contract_version': 2, 'literal_page_key_valid': stockfeel_url_key(page_url) is not None,
        'url_identity': {'contract_version': 1, 'canonical': canonical, 'open_graph': og_urls,
            'declaration_is_not_a_verified_alias_or_body_reference': True,
            'additional_URLs_enqueued_or_requested': False},
        'scope_count': len(scopes), 'examined_scope_bound': 2, 'header_count_within_bound': len(headers),
        'credit_count_within_bound': len(nodes), 'credits': credits, 'credits_truncated': len(nodes) > 8,
        'meta_author_count': len(authors), 'meta_authors': [named(str(node.get('content', ''))) for node in authors[:2]],
        'native_original_region_count_within_bound': len(natives),
        'written_tag_counts': {tag: tags[tag] for tag in sorted(_BODY_TAGS) if tags[tag]},
        'unreviewed_tag_counts': {tag: tags[tag] for tag in ('iframe', 'form', 'script', 'style', 'svg', 'video', 'audio') if tags[tag]},
        'other_unreviewed_tag_count': sum(count for tag, count in tags.items()
            if tag not in _BODY_TAGS | {'iframe', 'form', 'script', 'style', 'svg', 'video', 'audio'}),
        'copyright_node_count': len(notices), 'copyright_exact_known_declaration': len(notices) == 1
            and notices[0].get_text(' ', strip=True) == _COPYRIGHT,
        'source_clocks': clocks, 'body_text_retained': False, 'personal_names_retained': False,
        'raw_response_retained': False, 'article_or_rights_acceptance_granted': False,
        'additional_requests': 0}


def stockfeel_url_key(url):
    """Compare the publisher's Unicode/UTF-8 spellings, not guessed aliases."""
    if (not isinstance(url, str) or any(ord(value) < 33 for value in url)
            or '?' in url or '#' in url):
        return None
    try:
        parts = urlsplit(url)
        if (parts.scheme != 'https' or parts.netloc != 'www.stockfeel.com.tw'
                or parts.query or parts.fragment or re.search(r'%(?:2f|5c|25)', parts.path, re.I)
                or re.search(r'%(?![0-9a-fA-F]{2})', parts.path)):
            return None
        path = unquote(parts.path, encoding='utf-8', errors='strict')
    except (UnicodeError, ValueError):
        return None
    if (not path.startswith('/') or not path.endswith('/') or path.count('/') != 2
            or len(path) <= 2 or path[1:-1] in {'.', '..'} or '\\' in path
            or any(ord(value) < 33 for value in path)):
        return None
    return path


def _clock(value):
    try:
        if not isinstance(value, str) or not _CLOCK.fullmatch(value):
            raise ValueError('clock precision or offset not declared')
        return datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None


def _title_comparison_key(value):
    """Compare witnessed typography, never rewrite raw titles or minus signs.

    The observed editorial separator follows a parenthesized numeric stock
    code. Other dashes, symbols, Unicode normalization and fuzzy matches are
    deliberately not equivalent, especially signed financial values.
    """
    if (not isinstance(value, str) or not 0 < len(value) <= 2048
            or any(not char.isprintable() and char not in '\t\r\n' for char in value)):
        return None
    spaced = re.sub(r'[ \t\r\n]+', ' ', value).strip(' \t\r\n')
    return re.sub(r'(\( ?[0-9]{4,6} ?\)) – ', r'\1 - ', spaced)


def stockfeel_visible_original(soup, *, page_url, adapter_contract_version=1):
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3, 4, 5, 6, 7}:
        raise ValueError('unreviewed_stockfeel_capture_contract')
    key = stockfeel_url_key(page_url)
    canonical = _one(soup, 'head > link[rel=canonical]')
    og_url = _one(soup, 'head > meta[property="og:url"]')
    if key is None or any(stockfeel_url_key(value) != key
            for value in (canonical.get('href'), og_url.get('content'))):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, '.single-content > .row > .col-md-12 > .sin-content-wrapper')
    header = _one(scope, ':scope > .sin-content-header')
    heading = _one(header, ':scope > h1')
    title = heading.get_text(' ', strip=True)
    expected = title + ' - StockFeel 股感'
    document_title = _one(soup, 'head > title').get_text(' ', strip=True)
    og_titles = soup.select('head > meta[property="og:title"]')
    og_types = soup.select('head > meta[property="og:type"]')
    if (not title or not 1 <= len(og_titles) <= 2
            or not 1 <= len(og_types) <= 2 or any(node.get('content') != 'article' for node in og_types)):
        raise NewsError('article_identity_not_verified')
    title_notices = []
    if document_title != expected or any(node.get('content') != expected for node in og_titles):
        key = _title_comparison_key(expected) if adapter_contract_version >= 6 else None
        if (key is None or _title_comparison_key(document_title) != key
                or any(_title_comparison_key(node.get('content')) != key for node in og_titles)):
            raise NewsError('article_identity_not_verified')
        title_notices.append({'kind': 'source_declared_typographic_title_variants',
            'native_heading': title, 'document_title_raw': document_title,
            'OpenGraph_titles_raw': [node.get('content') for node in og_titles],
            'comparison_contract': 'ASCII_whitespace_and_parenthesized_stock_code_separator_en_dash_v6',
            'raw_titles_rewritten': False, 'historical_alias_or_identity_inferred': False})

    # Author and publisher-source are independent declarations. Old427 fields
    # stay exact under v3. A new writer must match native name/profile/avatar
    # and head declarations beside the exact source67 publisher, not claim
    # ownership, a verified person, rendered visibility or a reuse license.
    credits = header.select(':scope > .flex > .flex-wrap > div[data-aid]')
    if len(credits) != 2:
        raise NewsError('article_attribution_not_verified')
    declarations = []
    for node, label in zip(credits, ('作者', '文章來源')):
        labels = node.find_all('span', recursive=False)
        aid = node.get('data-aid')
        expected_name, expected_profile = _AUTHOR, _PROFILE
        source_role = label == '文章來源'
        independent_writer = not source_role and adapter_contract_version >= 3 and aid != '427'
        allowed_ids = {'427', '67'} if source_role and adapter_contract_version >= 2 else {'427'}
        if source_role and adapter_contract_version >= 2 and aid == '67':
            expected_name, expected_profile = '股感知識庫', STOCKFEEL_ORIGIN + '/author/stockfeel'
        unlinked_writer = adapter_contract_version >= 7 and not source_role and aid == ''
        if ((not independent_writer and aid not in allowed_ids)
                or independent_writer and not unlinked_writer and not (isinstance(aid, str) and re.fullmatch(r'[0-9]{1,8}', aid))
                or len(labels) != 2
                or labels[0].get_text(' ', strip=True) != label):
            raise NewsError('article_rights_or_scope_excluded')
        if unlinked_writer:
            # The publisher actually declares a name but no ID or profile.
            # Never manufacture a link or substitute the publisher as writer.
            value = labels[1]
            if (len(value.contents) != 1 or type(value.contents[0]) is not NavigableString
                    or set(value.attrs) - {'class'}
                    or set(value.get('class', [])) - {'f5', 'tet-gray-2', 'b'}):
                raise NewsError('article_attribution_not_verified')
            name_raw = str(value.contents[0])
            expected_name, expected_profile = name_raw.strip(), None
            if (not 1 <= len(expected_name) <= 128 or len(name_raw) > 256
                    or any(not char.isprintable() for char in name_raw) or '\ufffd' in name_raw
                    or expected_name in _HEAD_GROUP_DECLARATIONS | {'股感知識庫'}):
                raise NewsError('article_attribution_not_verified')
        else:
            link = _one(labels[1], ':scope > a', 'article_attribution_not_verified')
        if independent_writer and not unlinked_writer:
            expected_name = link.get_text(' ', strip=True)
            expected_profile = stockfeel_writer_profile(link.get('href'), adapter_contract_version=min(adapter_contract_version, 4))
            if (not expected_profile or not 1 <= len(expected_name) <= 128
                    or any(not char.isprintable() for char in expected_name)
                    or aid == '67' and (expected_name, expected_profile) != (
                        '股感知識庫', STOCKFEEL_ORIGIN + '/author/stockfeel')
                    or aid != '67' and (expected_name in {_AUTHOR, '股感知識庫'}
                        or expected_profile in {_PROFILE, STOCKFEEL_ORIGIN + '/author/stockfeel'})):
                raise NewsError('article_attribution_not_verified')
        if not unlinked_writer and (link.get('href') != expected_profile or link.get_text(' ', strip=True) != expected_name):
            raise NewsError('article_rights_or_scope_excluded')
        style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
        if style not in {'', 'visibility:hidden;'} or node.has_attr('hidden') or node.has_attr('aria-hidden'):
            raise NewsError('article_attribution_not_verified')
        declarations.append({'label': label, 'name': expected_name, 'profile_url': expected_profile,
            'data_aid': aid, 'initial_inline_visibility_hidden': bool(style),
            **({'name_text_raw': name_raw, 'numeric_id_declared': False, 'profile_declared': False,
                'attribution_basis': 'literal_named_unlinked_native_writer_not_verified_person_v7'} if unlinked_writer else {})})
    author = declarations[0]
    unlinked_writer = author['data_aid'] == ''
    independent_writer = author['data_aid'] != '427'
    if independent_writer and declarations[1]['data_aid'] != '67':
        raise NewsError('article_rights_or_scope_excluded')
    meta_author = _one(soup, 'head > meta[name=author]', 'article_attribution_not_verified').get('content')
    if unlinked_writer and meta_author != declarations[1]['name']:
        raise NewsError('article_attribution_not_verified')
    names, credit_detail = [author['name']], {}
    if meta_author != author['name']:
        if (adapter_contract_version < 5 or not independent_writer or declarations[1]['data_aid'] != '67'
                or not isinstance(meta_author, str) or not 1 <= len(meta_author) <= 512
                or any(not char.isprintable() for char in meta_author)):
            raise NewsError('article_attribution_not_verified')
        declared_names = meta_author.split(', ')
        if (2 <= len(declared_names) <= 4 and declared_names[0] == author['name']
                and len(set(declared_names)) == len(declared_names)
                and all(1 <= len(name) <= 128 and name == name.strip() and ',' not in name for name in declared_names)):
            names = declared_names
            relationship = 'native_primary_writer_and_additional_head_declared_authors'
        elif ((meta_author in _HEAD_GROUP_DECLARATIONS
                or adapter_contract_version >= 7 and meta_author == declarations[1]['name'])
                and author['name'] not in _HEAD_GROUP_DECLARATIONS | {'股感知識庫'}):
            relationship = 'native_writer_and_distinct_head_group_declaration'
        else:
            raise NewsError('article_attribution_not_verified')
        credit_detail = {'credit_relationship': relationship, 'head_declared_names': declared_names,
            'head_declaration_equals_primary_native_writer': False,
            'additional_head_profile_or_person_identity_verified': False}
    avatar = _one(header, ':scope > .flex > a.user-avatar', 'article_attribution_not_verified')
    expected_avatar = declarations[1]['profile_url'] if unlinked_writer else author['profile_url']
    if avatar.get('href') != expected_avatar:
        raise NewsError('article_attribution_not_verified')
    new_credit_roles = unlinked_writer or (adapter_contract_version >= 7 and meta_author == declarations[1]['name']
        and meta_author != author['name'])
    if new_credit_roles:
        credit_detail.update({'credit_contract': 'native_writer_and_distinct_publisher_head_group_v7',
            'native_writer_profile_declared': not unlinked_writer, 'native_writer_numeric_id_declared': not unlinked_writer,
            'avatar_profile_url_raw': avatar.get('href'),
            'avatar_role': 'declared_publisher_group_not_native_writer' if unlinked_writer else 'declared_native_writer',
            'publisher_head_group_is_not_native_writer': True})

    published = _one(soup, 'head > meta[property="article:published_time"]', 'invalid_bulk_publication_clock').get('content')
    modified = _one(soup, 'head > meta[property="article:modified_time"]', 'invalid_bulk_publication_clock').get('content')
    if _clock(modified) < _clock(published):
        raise NewsError('invalid_bulk_publication_clock')
    displayed = _one(header, ':scope > p.text-gray-3', 'invalid_bulk_publication_clock').get_text(' ', strip=True)
    # Layout whitespace does not add time precision or a timezone. Preserve
    # the original declaration while parsing only its normalized calendar.
    display_match = re.fullmatch(r'最近更新時間： ([0-9]{1,2}) ([A-Za-z]+), ([0-9]{4})',
                                 ' '.join(displayed.split()))
    if not display_match:
        raise NewsError('invalid_bulk_publication_clock')
    try:
        months = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
                  'September', 'October', 'November', 'December')
        day, month, year = display_match.groups()
        datetime(int(year), months.index(month) + 1, int(day))
    except (ValueError, TypeError):
        raise NewsError('invalid_bulk_publication_clock') from None

    native = _one(scope, STOCKFEEL_BODY_SELECTOR)
    body_elements = native.find_all()
    for node in (scope, header, heading, avatar, native, *body_elements):
        _visible(node)
        if any(set(parent.get('class', [])) & {'hidden', 'invisible', 'sr-only'}
                for parent in (node, *node.parents) if getattr(parent, 'attrs', None)):
            raise NewsError('article_identity_not_verified')
    if native.select('iframe,form,[data-paywall],.paywall,vip-content'):
        raise NewsError('paywall_or_login')
    chart_notices = _interactive_chart_notices(native) if adapter_contract_version >= 5 else []
    extra_tags = ({'wbr'} | ({'script'} if chart_notices else set())) if adapter_contract_version >= 5 else set()
    if (any(node.name not in _BODY_TAGS | extra_tags for node in body_elements)
            or any(node.attrs or node.contents for node in native.find_all('wbr'))):
        raise NewsError('article_identity_not_verified')
    for node in native.select('p,blockquote'):
        if re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', node.get_text(' ', strip=True)):
            raise NewsError('article_rights_or_scope_excluded')
    # Empty advertising carousel is inert markup, not a second text source.
    for widget in native.select('.swiper.article-bottom'):
        if widget.get_text(strip=True) or widget.select('img,script,iframe,form'):
            raise NewsError('article_identity_not_verified')
    copyright_node = _one(soup, 'footer .copyright > span', 'article_rights_not_verified')
    _visible(copyright_node)
    copyright_text = copyright_node.get_text(' ', strip=True)
    if copyright_text != _COPYRIGHT:
        raise NewsError('article_rights_not_verified')
    return {
        'headline': title, 'author': [{'name': name} for name in names], 'datePublished': published, 'dateModified': modified,
        'attribution_status': 'source_declared_header_credit_not_render_verified',
        'author_attribution_detail_json': json.dumps({'source_header_declarations': declarations,
            'meta_author_raw': meta_author, 'attribution_basis': ('independent_native_writer_and_publisher_declarations_missing_profile_retained_v7'
                if new_credit_roles else 'independent_native_and_head_declarations_with_explicit_role_difference_v5'
                if credit_detail else 'matching_native_header_and_head_declarations'), **credit_detail,
            'rendered_byline_visibility_verified': False, 'human_reporter_identity_verified': False,
            **({'declared_writer_not_independently_verified_person': True} if independent_writer else
               {'source_group_not_independently_verified_person': True})}, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps([
            {'kind': 'source_displayed_revision_calendar', 'text': displayed,
             'timezone_declared': False, 'not_a_publication_or_PIT_observation': True},
            {'kind': 'publisher_copyright_notice', 'text': copyright_text,
             'reuse_or_open_license_inferred': False}, *chart_notices, *title_notices], ensure_ascii=False, sort_keys=True),
        'published_time_basis': 'explicit_source_OpenGraph_publication_seconds_and_offset; visible_revision_calendar_retained_separately; not_historical_PIT_or_reported_event_date',
        'article_identity_basis': ('single_canonical_and_OG_UTF8_path_bounded_title_comparison_v6; exact_source67_publisher_and_separately_declared_writer_roles_v7; missing_writer_ID_profile_stay_missing; not_ownership_license_historical_identity_or_guessed_alias'
            if new_credit_roles else
            'single_canonical_and_OG_UTF8_path_native_and_declared_titles_match_only_ASCII_whitespace_and_parenthesized_stock_code_separator_v6; independently_verified_native_credit_declarations; not_ownership_license_historical_identity_or_guessed_alias'
            if title_notices else
            'same_native_h1_head_and_all_OG_titles_single_canonical_and_OG_UTF8_path_matching_writer_name_profile_avatar_and_exact_source67_v' + str(min(adapter_contract_version, 4)) + '; declared_roles_not_copyright_ownership_or_license; not_a_guessed_alias'
            if independent_writer else
            'same_native_h1_head_and_all_OG_titles_single_canonical_and_OG_UTF8_path_author427_independent_source67_v2; declared_roles_not_copyright_ownership_or_license; not_a_guessed_alias'
            if declarations[1]['data_aid'] == '67' else
            'same_native_h1_head_and_all_OG_titles_single_canonical_and_OG_UTF8_path_original_427_credit_pair_v1; not_a_guessed_alias'),
        'body_completeness': ('whole_current_native_written_prose_tables_and_in_body_links_once; initial_hidden_credits_declared_not_render_verified; media_scripts_and_unpublished_API_not_requested; not_original_historical_version'
            + ('; reviewed_empty_wbr_written_boundary_v5' if native.find('wbr') else '')
            + ('; reviewed_interactive_chart_configuration_retained_not_executed_or_chart_values_captured_v5' if chart_notices else '')),
    }, scope
