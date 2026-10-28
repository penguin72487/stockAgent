"""Once-only, body-free observations of known Moneyweekly legacy routes.

This is a question for the canonical HTTP owner, not a URL converter. A literal
Location/canonical is source evidence; it never proves aliases, permission to
fetch a destination, article completeness, or historical publication.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime
from urllib.parse import unquote, urljoin, urlsplit

from downloader.news_fulltext_policy import NewsError, normalize_url

ROUTE_METADATA_LAYOUT = 'moneyweekly_known_route_metadata_v1'
ROUTE_METADATA_QUESTION = 'literal_known_moneyweekly_first_hop_and_public_header_v1'
BODY_GEOMETRY_LAYOUT = 'moneyweekly_known_body_geometry_v1'
BODY_GEOMETRY_QUESTION = 'literal_known_moneyweekly_native_body_geometry_v1'
LEGACY_ARTICLE_PATTERN = r'^/(?:ArticleData/Info|Home/ChangePage)/(?:(?i:article)|(?i:%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A)|理財周刊)/[0-9]+/?$'
NATIVE_ARTICLE_PATTERN = r'^/(?:(?:ArticleData/Info|Home/ChangePage)/(?:(?i:article)|(?i:%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A)|理財周刊)/[0-9]+/?|Magazine/Info/(?:(?i:%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A)|理財周刊)/[0-9]+/?|_Article)$'
NATIVE_ROUTE_CONTRACT_VERSION = 5


def moneyweekly_route_version(source):
    return NATIVE_ROUTE_CONTRACT_VERSION if source.article_path_pattern == NATIVE_ARTICLE_PATTERN else 3


def moneyweekly_article_id(url, *, version=3, requested=False):
    if not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url)
        if (parts.hostname not in {'www.moneyweekly.com.tw', 'moneyweekly.com.tw'}
                or parts.scheme not in {'http', 'https'} or parts.username or parts.password
                or parts.port not in {None, 80 if parts.scheme == 'http' else 443}
                or re.search(r'%(?:2f|5c)', parts.path, re.I)):
            return None
        old = re.fullmatch(r'/(?:ArticleData/Info|Home/ChangePage)/(?:(?i:article)|理財周刊)/([1-9][0-9]*)/?',
            unquote(parts.path))
        if old:
            return old[1]  # Preserve the existing literal query/provenance semantics.
        if version not in {4, 5} or not moneyweekly_literal_unhandled_route(url):
            return None
        if parts.path == '/_Article':
            return parts.query.removeprefix('AID=') if requested else None
        return unquote(parts.path).rstrip('/').rsplit('/', 1)[-1]
    except (ValueError, NewsError):
        return None


def validate_moneyweekly_native_response(source, requested_url, response):
    """Validate the witnessed view/redirect, never rewrite a requested URL."""
    if moneyweekly_route_version(source) < 4 or not moneyweekly_literal_unhandled_route(requested_url):
        return None
    if urlsplit(requested_url).path != '/_Article':
        if response.url != requested_url or response.redirects:
            raise NewsError('article_identity_not_verified')
        return None
    hops = response.redirects
    destination = urlsplit(response.url)
    try:
        # HTTP Location is a URI reference. Resolve the actual source header
        # against its actual first hop; do not invent a destination by ID.
        declared = normalize_url(urljoin(hops[0].from_url, hops[0].location)) if len(hops) == 1 else None
    except (ValueError, TypeError, NewsError):
        raise NewsError('article_identity_not_verified') from None
    if (len(hops) != 1 or hops[0].status != 302 or hops[0].from_url != requested_url
            or hops[0].to_url != response.url or declared != response.url
            or destination.scheme != 'https' or destination.netloc != 'www.moneyweekly.com.tw'
            or destination.query or destination.fragment
            or moneyweekly_article_id(response.url) != moneyweekly_article_id(requested_url, version=4, requested=True)):
        raise NewsError('article_identity_not_verified')
    return 'observed_moneyweekly_same_id_source_redirect_v5; literal_request_and_redirect_retained'


def moneyweekly_route_source(source):
    return bool(source and source.provider == 'moneyweekly' and source.allowed
        and source.domains == ('moneyweekly.com.tw',) and source.known_url_only
        and source.acquisition_mode == 'continuous' and not source.embedded_article_layout
        and source.selectors == ('.div_Article_Info[itemprop=articleBody]',)
        and source.require_configured_container and source.require_author_attribution
        and source.permitted_author_names
        and source.authorization.get('kind') == 'personal_research'
        and source.authorization.get('evidence_url') ==
            'https://www.moneyweekly.com.tw/ArticleData/Info/%E7%BD%AE%E5%BA%95%E6%96%87%E7%AB%A0/7')


def moneyweekly_literal_unhandled_route(url):
    if not isinstance(url, str):
        return False
    try:
        parts = urlsplit(url)
        if (normalize_url(url) != url or parts.scheme != 'https'
                or parts.netloc != 'www.moneyweekly.com.tw' or parts.fragment):
            return False
        return bool(
            parts.path == '/_Article' and re.fullmatch(r'AID=[1-9][0-9]*', parts.query)
            or not parts.query and re.fullmatch(
                r'/Magazine/Info/(?:理財周刊|%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A)/[1-9][0-9]*/?',
                parts.path))
    except (ValueError, NewsError):
        return False


def _declared_url(value, base):
    """Bound public URL declarations without retaining credentials or prose."""
    if not isinstance(value, str) or not 0 < len(value) <= 2048:
        return None
    try:
        url = urljoin(base, value)
        parts = urlsplit(url)
        if (parts.scheme not in {'http', 'https'} or not parts.hostname
                or parts.username or parts.password or parts.fragment
                or normalize_url(url) != url):
            return None
        return url
    except (ValueError, NewsError):
        return None


def moneyweekly_route_metadata(response, source, requested_url):
    navigation = moneyweekly_literal_navigation_route(requested_url)
    if (not moneyweekly_route_source(source)
            or not moneyweekly_literal_unhandled_route(requested_url) and not navigation
            or navigation and not moneyweekly_body_geometry_source(source)
            or response.url != requested_url or response.redirects):
        raise NewsError('literal_moneyweekly_first_hop_required')
    if navigation and response.status == 404:
        raise NewsError('http_404')
    if response.status not in {200, 301, 302, 303, 307, 308}:
        raise NewsError('literal_moneyweekly_first_hop_required')
    facts = dict(contract_version=1, evidence_contract_version=64,
        provider='moneyweekly', question=ROUTE_METADATA_QUESTION,
        requested_url=requested_url, final_url=response.url, status=response.status,
        redirect_count=0, response_bytes=len(response.body),
        response_sha256=hashlib.sha256(response.body).hexdigest(),
        current_source_not_candidate=True, accepted_as_article=False,
        body_completeness_verified=False, identity_or_alias_accepted=False,
        raw_or_names_or_body_retained=False, raw_or_body_retained=False,
        destination_requested=False)
    location = response.headers.get('location')
    declared = _declared_url(location, requested_url)
    if navigation and declared:
        parts = urlsplit(declared)
        # This is the publisher's actual anonymous Location, not a decoded
        # Page value or an inferred same-ID alias. An opaque query remains
        # private source metadata; it grants no author, body or URL mapping.
        if (parts.scheme != 'https' or parts.netloc != 'www.moneyweekly.com.tw'
                or not re.fullmatch(r'/ArticleData/Info/Article/[1-9][0-9]*/?', parts.path)):
            declared = None
    facts['location'] = declared
    facts['location_raw_sha256'] = (hashlib.sha256(location.encode()).hexdigest()
        if isinstance(location, str) else None)
    if navigation:
        facts['opaque_query_semantics_accepted'] = False
        facts['location_reference_private'] = True
        return facts
    if response.status == 200:
        from bs4 import BeautifulSoup
        # This registered-host metadata question deliberately does not admit
        # its path to the article extractor. Keep the HTML representation gate
        # without relaxing _require_article_response's production path gate.
        mime = response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
        if mime not in {'text/html', 'application/xhtml+xml'}:
            raise NewsError('unsupported_content_type')
        soup = BeautifulSoup(response.body, 'lxml')
        facts['canonical_urls'] = [_declared_url(n.get('href'), requested_url)
            for n in soup.select('head > link[rel=canonical]')[:4]]
        facts['og_urls'] = [_declared_url(n.get('content'), requested_url)
            for n in soup.select('head > meta[property="og:url"]')[:4]]
        facts['heading_count'] = len(soup.select('h1'))
        bodies = soup.select('.div_Article_Info[itemprop=articleBody]')
        facts['configured_body_count'] = len(bodies)
        facts['configured_body_characters'] = [len(n.get_text(' ', strip=True)) for n in bodies[:4]]
        facts['native_member_mask_present'] = any(
            '本文限會員登入瀏覽' in n.get_text(' ', strip=True) for n in bodies)
    return facts


def moneyweekly_body_geometry_source(source):
    return moneyweekly_route_source(source) and source.article_path_pattern == NATIVE_ARTICLE_PATTERN


def moneyweekly_body_geometry_route(url):
    """Literal known views only; FN's original navigation query is not decoded."""
    if not isinstance(url, str) or not 0 < len(url) <= 4096:
        return False
    try:
        parts = urlsplit(url)
        if (normalize_url(url) != url or parts.scheme not in {'http', 'https'}
                or parts.netloc != 'www.moneyweekly.com.tw' or parts.fragment
                or re.search(r'%(?:2f|5c)', parts.path, re.I)):
            return False
        path = unquote(parts.path)
        if re.fullmatch(r'/Home/ChangePage/FN/[1-9][0-9]*/?', path):
            query = re.fullmatch(r'Type=0&Line=Include_FirstNews_[1-9][0-9]*&Page=([0-9A-F]{2,2048})', parts.query)
            return bool(query and len(query[1]) % 2 == 0)
        return not parts.query and bool(re.fullmatch(
            r'/(?:Magazine/Info/理財周刊|ArticleData/Info/(?:理周新聞|說解財經大小事|觀點文章))/[1-9][0-9]*/?', path))
    except (ValueError, NewsError):
        return False


def moneyweekly_literal_navigation_route(url):
    return bool(moneyweekly_body_geometry_route(url)
        and urlsplit(url).path.startswith('/Home/ChangePage/FN/'))


def moneyweekly_navigation_metadata_result(result, url):
    """An opaque private URI reference is never a prose or alias grant."""
    try:
        false = ('accepted_as_article', 'body_completeness_verified', 'identity_or_alias_accepted',
            'raw_or_names_or_body_retained', 'raw_or_body_retained', 'destination_requested',
            'opaque_query_semantics_accepted')
        fields = {'contract_version', 'evidence_contract_version', 'provider', 'question', 'requested_url',
            'final_url', 'status', 'redirect_count', 'response_bytes', 'response_sha256',
            'current_source_not_candidate', 'location', 'location_raw_sha256', 'location_reference_private', *false}
        if (not moneyweekly_literal_navigation_route(url) or not isinstance(result, dict)
                or set(result)-{'timing'} != fields
                or type(result['contract_version']) is not int or result['contract_version'] != 1
                or type(result['evidence_contract_version']) is not int or result['evidence_contract_version'] != 64
                or result['provider'] != 'moneyweekly' or result['question'] != ROUTE_METADATA_QUESTION
                or result['requested_url'] != url or result['final_url'] != url
                or type(result['status']) is not int or result['status'] not in {200,301,302,303,307,308}
                or type(result['redirect_count']) is not int or result['redirect_count'] != 0
                or type(result['response_bytes']) is not int or not 0 <= result['response_bytes'] <= 16 << 20
                or result['current_source_not_candidate'] is not True or result['location_reference_private'] is not True
                or any(result[key] is not False for key in false)
                or not isinstance(result['response_sha256'], str) or not re.fullmatch('[0-9a-f]{64}', result['response_sha256'])):
            return False
        declared, digest = result['location'], result['location_raw_sha256']
        if digest is not None and (not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest)):
            return False
        if declared is not None:
            if not isinstance(declared, str) or digest is None or _declared_url(declared, url) != declared:
                return False
            parts = urlsplit(declared)
            if (parts.scheme != 'https' or parts.netloc != 'www.moneyweekly.com.tw'
                    or not re.fullmatch(r'/ArticleData/Info/Article/[1-9][0-9]*/?', parts.path)):
                return False
        if 'timing' in result:
            timing = result['timing']
            if (not isinstance(timing, dict) or set(timing) != {'http_seconds','parse_seconds','scope_seconds','total_seconds'}
                    or any(type(v) not in (int,float) or not math.isfinite(v) or not 0 <= v <= 3600 for v in timing.values())):
                return False
        return True
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError):
        return False


def require_prior_moneyweekly_navigation(db, source, url):
    """Continue only a closed, still withheld declaration; never rearm it.

    The completed v65 geometry and the v64 first-hop declaration are separate
    fixed questions. Capture-only negative classifications may preserve the
    same parser/authorization/HTTP predecessor; arbitrary source changes may
    not inherit this evidence.
    """
    from downloader.news_fulltext_repairs import _bytes, _clock, _head, _source_contract
    try:
        if not moneyweekly_body_geometry_source(source) or not moneyweekly_literal_navigation_route(url):
            raise ValueError
        key = hashlib.sha256(_bytes([65, source.provider, hashlib.sha256(url.encode()).hexdigest(),
            BODY_GEOMETRY_QUESTION])).hexdigest()
        row = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        head = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        if (row is None or row['provider'] != source.provider or row['state'] != 'observed'
                or head is None or head['state'] != 'unsupported_article_path'
                or head['error_code'] != 'unsupported_article_path'
                or len(row['request_json']) > 131072 or len(row['result_json']) > 131072):
            raise ValueError
        request, result = json.loads(row['request_json']), json.loads(row['result_json'])
        capture = request['source']['data_fingerprint']
        history = {source.data_fingerprint, source.parser_repair_prior_capture_fingerprint,
            *source.parser_repair_capture_history}
        old_source = replace(source, data_fingerprint=capture, parser_repair_capture_fingerprint=capture)
        if (capture not in history or request['source'] != _source_contract(db, old_source, url)[1]
                or request['contract_version'] != 65 or request['evidence_contract_version'] != 65
                or request['candidate_layout'] != BODY_GEOMETRY_LAYOUT
                or request['candidate_fingerprint'] != capture or request['target'] != _head(head)
                or hashlib.sha256(_bytes(request)).hexdigest() != row['request_id']
                or not moneyweekly_body_geometry_result(result, url)
                or result['status'] != 302 or result['location'] is not None
                or not isinstance(result['location_raw_sha256'], str)
                or not _clock(row['created_at_utc']) <= _clock(row['reserved_at_utc'])
                    <= _clock(row['completed_at_utc']) <= time.time()):
            raise ValueError
        return source
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError, RecursionError, sqlite3.OperationalError):
        raise ValueError('closed_withheld_moneyweekly_navigation_required') from None


def _geometry_declared_url(value, base):
    declared = _declared_url(value, base)
    if declared and (moneyweekly_body_geometry_route(declared)
            or moneyweekly_literal_unhandled_route(declared)
            or not urlsplit(declared).query and moneyweekly_article_id(declared)):
        return declared
    return None


_GEOMETRY_TAGS = frozenset({'p', 'div', 'span', 'img', 'a', 'br', 'figure', 'figcaption',
    'strong', 'b', 'i', 'em', 'ul', 'ol', 'li', 'table', 'tbody', 'tr', 'td', 'th',
    'h1', 'h2', 'h3', 'h4', 'script', 'style', 'iframe', 'video', 'audio', 'svg', 'noscript', 'other'})


def _geometry_tag(node):
    return node.name if node.name in _GEOMETRY_TAGS else 'other'


def _geometry_shape(node, source):
    from downloader.news_fulltext_policy import _plain_text_node
    from downloader.news_fulltext_public_originals import _visible
    text = _plain_text_node(node, source.exclude_selectors, parser=source.html_parser)
    try:
        _visible(node)
        hidden = False
    except NewsError:
        hidden = True
    return dict(tag=_geometry_tag(node), text_characters=len(text),
        text_sha256=hashlib.sha256(text.encode()).hexdigest(), inline_hidden_hint=hidden)


def _geometry_body(node, source):
    from bs4 import Tag
    descendants = node.find_all()
    counts = Counter(_geometry_tag(child) for child in descendants)
    children = [child for child in node.children if isinstance(child, Tag)]
    paragraphs = [child for child in descendants if child.name == 'p']
    return dict(_geometry_shape(node, source),
        descendant_tag_counts=dict(sorted(counts.items())), paragraph_count=len(paragraphs),
        image_count=counts['img'], iframe_count=counts['iframe'], script_count=counts['script'],
        direct_children_count=len(children), direct_children=[_geometry_shape(child, source) for child in children[:32]],
        paragraphs=[_geometry_shape(child, source) for child in paragraphs[:16]],
        children_truncated=len(children) > 32, paragraphs_truncated=len(paragraphs) > 16)


def moneyweekly_body_geometry(response, source, requested_url):
    """Observe bounded hashes/counts, without body retention or article admission.

    This separate fixed question explains an unresolved native editor. It does
    not rearm the completed first-hop question, follow a redirect, or change a
    source/head. Visibility hints describe HTML attributes, not rendered CSS.
    """
    if (not moneyweekly_body_geometry_source(source) or not moneyweekly_body_geometry_route(requested_url)
            or response.url != requested_url or response.redirects):
        raise NewsError('literal_moneyweekly_body_geometry_required')
    if response.status == 404:
        # A literal source 404 is a source observation, not a bad candidate
        # contract. Preserve its actual status in the once-only error record.
        raise NewsError('http_404')
    if response.status not in {200, 301, 302, 303, 307, 308}:
        raise NewsError('literal_moneyweekly_body_geometry_required')
    location = response.headers.get('location')
    facts = dict(contract_version=1, evidence_contract_version=65, provider='moneyweekly',
        question=BODY_GEOMETRY_QUESTION, requested_url=requested_url, final_url=response.url,
        status=response.status, redirect_count=0, response_bytes=len(response.body),
        response_sha256=hashlib.sha256(response.body).hexdigest(), current_source_not_candidate=True,
        accepted_as_article=False, body_completeness_verified=False, identity_or_alias_accepted=False,
        raw_or_names_or_body_retained=False, raw_or_body_retained=False, raw_response_retained=False,
        full_body_retained=False, destination_requested=False,
        location=_geometry_declared_url(location, requested_url),
        location_raw_sha256=hashlib.sha256(location.encode()).hexdigest() if isinstance(location, str) else None,
        html=None)
    if response.status != 200:
        return facts
    if response.headers.get('content-type', '').split(';', 1)[0].strip().lower() not in {'text/html', 'application/xhtml+xml'}:
        raise NewsError('unsupported_content_type')
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.body, source.html_parser)
    bodies = soup.select('.div_Article_Info[itemprop=articleBody]')
    authors = [node for selector in source.author_selectors for node in soup.select(selector)]
    dates = [node for selector in source.publication_selectors for node in soup.select(selector)]
    day_valid = False
    if len(dates) == 1:
        try:
            datetime.strptime(dates[0].get_text(' ', strip=True), '%Y/%m/%d')
            day_valid = True
        except ValueError:
            pass
    html = dict(heading_count=len(soup.select('h1')), configured_body_count=len(bodies),
        author_field_count=len(authors), author_matches_permitted=len(authors) == 1
            and authors[0].get_text(' ', strip=True) in source.permitted_author_names,
        publication_field_count=len(dates), publication_day_valid=day_valid,
        bodies=[_geometry_body(node, source) for node in bodies[:2]], body_summaries_truncated=len(bodies) > 2,
        native_member_mask_present=any(re.fullmatch(r'》本文限會員登入瀏覽，請先登入(?:\s+登入會員)?',
            node.get_text(' ', strip=True)) is not None
            for body in bodies for node in body.find_all('div', class_='mt-5', recursive=False)))
    for key, selector, attribute in (('canonical', 'head > link[rel=canonical]', 'href'),
                                    ('og', 'head > meta[property="og:url"]', 'content')):
        nodes = soup.select(selector)
        html[key + '_count'] = len(nodes)
        html[key + '_declarations'] = [dict(url=_geometry_declared_url(node.get(attribute), requested_url),
            raw_sha256=hashlib.sha256(node[attribute].encode()).hexdigest()
                if isinstance(node.get(attribute), str) else None) for node in nodes[:4]]
    facts['html'] = html
    return facts


def moneyweekly_body_geometry_result(result, url):
    """Reject arbitrary strings/fields at the durable metadata boundary."""
    import math

    def keys(value, expected):
        return isinstance(value, dict) and set(value) == set(expected.split())

    def number(value):
        return type(value) is int and 0 <= value <= 1 << 31

    def digest(value):
        return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None

    def shape(value):
        return (keys(value, 'tag text_characters text_sha256 inline_hidden_hint')
            and value['tag'] in _GEOMETRY_TAGS and number(value['text_characters'])
            and digest(value['text_sha256']) and type(value['inline_hidden_hint']) is bool)

    false_flags = ('accepted_as_article', 'body_completeness_verified', 'identity_or_alias_accepted',
        'raw_or_names_or_body_retained', 'raw_or_body_retained', 'raw_response_retained',
        'full_body_retained', 'destination_requested')
    common = ('contract_version evidence_contract_version provider question requested_url final_url status '
        'redirect_count response_bytes response_sha256 current_source_not_candidate location location_raw_sha256 html '
        + ' '.join(false_flags))
    try:
        base = {key: value for key, value in result.items() if key != 'timing'}
        if (not keys(base, common) or result['contract_version'] != 1 or result['evidence_contract_version'] != 65
                or result['provider'] != 'moneyweekly' or result['question'] != BODY_GEOMETRY_QUESTION
                or not moneyweekly_body_geometry_route(url) or result['requested_url'] != url or result['final_url'] != url
                or result['status'] not in {200, 301, 302, 303, 307, 308} or result['redirect_count'] != 0
                or not number(result['response_bytes']) or not digest(result['response_sha256'])
                or result['current_source_not_candidate'] is not True or any(result[key] is not False for key in false_flags)
                or result['location'] is not None and _geometry_declared_url(result['location'], url) != result['location']
                or result['location_raw_sha256'] is not None and not digest(result['location_raw_sha256'])):
            return False
        html = result['html']
        if result['status'] != 200:
            if html is not None:
                return False
        else:
            if (not keys(html, 'heading_count configured_body_count author_field_count author_matches_permitted '
                    'publication_field_count publication_day_valid bodies body_summaries_truncated '
                    'native_member_mask_present canonical_count canonical_declarations og_count og_declarations')
                    or any(not number(html[key]) for key in ('heading_count', 'configured_body_count',
                        'author_field_count', 'publication_field_count', 'canonical_count', 'og_count'))
                    or any(type(html[key]) is not bool for key in ('author_matches_permitted',
                        'publication_day_valid', 'body_summaries_truncated', 'native_member_mask_present'))
                    or not isinstance(html['bodies'], list) or len(html['bodies']) != min(html['configured_body_count'], 2)
                    or html['body_summaries_truncated'] != (html['configured_body_count'] > 2)):
                return False
            for key in ('canonical', 'og'):
                declarations = html[key + '_declarations']
                if not isinstance(declarations, list) or len(declarations) != min(html[key + '_count'], 4):
                    return False
                for declaration in declarations:
                    if (not keys(declaration, 'url raw_sha256')
                            or declaration['url'] is not None and _geometry_declared_url(declaration['url'], url) != declaration['url']
                            or declaration['raw_sha256'] is not None and not digest(declaration['raw_sha256'])):
                        return False
            for body in html['bodies']:
                if (not keys(body, 'tag text_characters text_sha256 inline_hidden_hint descendant_tag_counts '
                        'paragraph_count image_count iframe_count script_count direct_children_count direct_children '
                        'paragraphs children_truncated paragraphs_truncated')
                        or not shape({key: body[key] for key in ('tag', 'text_characters', 'text_sha256', 'inline_hidden_hint')})
                        or any(not number(body[key]) for key in ('paragraph_count', 'image_count', 'iframe_count', 'script_count', 'direct_children_count'))
                        or not isinstance(body['descendant_tag_counts'], dict)
                        or set(body['descendant_tag_counts']) - _GEOMETRY_TAGS
                        or any(not number(value) for value in body['descendant_tag_counts'].values())):
                    return False
                for field, count, bound in (('direct_children', 'direct_children_count', 32), ('paragraphs', 'paragraph_count', 16)):
                    if (not isinstance(body[field], list) or len(body[field]) != min(body[count], bound)
                            or not all(shape(node) for node in body[field])):
                        return False
                for field, count, bound in (('children_truncated', 'direct_children_count', 32), ('paragraphs_truncated', 'paragraph_count', 16)):
                    if type(body[field]) is not bool or body[field] != (body[count] > bound):
                        return False
        if 'timing' in result:
            if (not keys(result['timing'], 'http_seconds parse_seconds scope_seconds total_seconds')
                    or any(type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 3600
                        for value in result['timing'].values())):
                return False
        return True
    except (AttributeError, KeyError, TypeError, ValueError, RecursionError):
        return False
