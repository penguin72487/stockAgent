"""Bounded metadata for known non-School Pocket pages, never article admission.

The failed article head and its clocks remain untouched. These observations
can distinguish an ETF/video document from a School parser failure without
retaining proprietary market data, names, text, or following embedded media.
"""
from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import parse_qsl, urljoin, urlsplit

from downloader.news_fulltext_policy import NewsError


QUESTION = 'pocket_non_school_document_boundary_v1'
MAX_BYTES = 2 << 20
_ORIGIN = 'https://www.pocket.tw'
_ETF = re.compile(r'/etf/{1,2}tw/([A-Z0-9]{4,8})(?:/(intro|fundholding|discountpremium|cashdividend))?/?')
_VIDEO = re.compile(r'/school/videos/([1-9][0-9]*)/')
_TYPES = frozenset({'Article', 'NewsArticle', 'BlogPosting', 'VideoObject',
    'WebPage', 'WebSite', 'FinancialProduct', 'InvestmentOrDeposit', 'BreadcrumbList'})
_LABELS = frozenset({'基本資料', '基金持股', '持股明細', '配息資訊', '配息紀錄',
    '折溢價', '現金股利', '影片專區', 'ETF基本資料', '持股', '成分股'})


def non_school_route(url):
    """An explicit diagnostic route, not a new data/authorization scope."""
    if not isinstance(url, str):
        return None
    parts = urlsplit(url)
    if parts.scheme+'://'+parts.netloc != _ORIGIN or parts.fragment:
        return None
    if len(parts.query) > 512:
        return None
    query = parse_qsl(parts.query, keep_blank_values=True)
    if len(query) > 6 or any(key not in {'page', 'parent', 'source'} for key, _ in query):
        return None
    match = _ETF.fullmatch(parts.path)
    if match:
        return ('etf_information', match[1], match[2] or 'overview')
    match = _VIDEO.fullmatch(parts.path)
    if match and not parts.query:
        return ('video_document', match[1], 'video')
    return None


def require_non_school_response(response, requested_url):
    """Same literal document/section, with witnessed same-origin slash hops.

    No cross-fund, cross-section, external, query-changing or guessed alias
    is accepted. Metadata validation cannot become normal article acceptance.
    """
    route = non_school_route(requested_url)
    if route is None or non_school_route(response.url) != route:
        raise NewsError('candidate_non_school_route_not_verified')
    previous = requested_url
    if len(response.redirects) > 3:
        raise NewsError('candidate_non_school_transport_not_verified')
    for hop in response.redirects:
        if (hop.from_url != previous or hop.status not in {301, 307, 308}
                or urljoin(hop.from_url, hop.location) != hop.to_url
                or non_school_route(hop.to_url) != route
                or urlsplit(hop.to_url).query != urlsplit(requested_url).query):
            raise NewsError('candidate_non_school_transport_not_verified')
        previous = hop.to_url
    if previous != response.url:
        raise NewsError('candidate_non_school_transport_not_verified')
    if response.status != 200:
        raise NewsError('http_'+str(response.status))
    mime = response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
    if mime not in {'text/html', 'application/xhtml+xml'}:
        raise NewsError('unsupported_content_type')
    if not response.body or len(response.body) > MAX_BYTES:
        raise NewsError('candidate_response_exceeds_bound')


def non_school_metadata(soup, *, page_url):
    """One bounded structural parse. Hashes/counts are not a body/scope grant."""
    route = non_school_route(page_url)
    if route is None:
        raise ValueError('literal_non_school_pocket_route_required')
    mains = soup.select('main')
    facts = {'contract_version': 1, 'route_kind': route[0], 'route_section': route[2],
        'accepted_as_article': False, 'authorization_or_identity_expanded': False,
        'raw_names_or_body_retained': False, 'financial_values_exported': False,
        'embedded_media_fetched': False, 'main_count': len(mains),
        'native_school_body_count': len(soup.select('article.cms-body > .cms-article-scope')),
        'schema_objects_seen': 0, 'schema_parse_errors': 0, 'schema_bound_exceeded': False,
        'known_schema_type_counts': {key: 0 for key in sorted(_TYPES)}, 'other_schema_type_count': 0}
    for key, selector, attr in (('canonical', 'head > link[rel=canonical]', 'href'),
            ('og_url', 'head > meta[property="og:url"]', 'content')):
        nodes = soup.select(selector)
        facts[key] = {'count': len(nodes), 'literal_page_URL_agrees':
            len(nodes) == 1 and nodes[0].get(attr) == page_url}
    scripts = soup.find_all('script', type='application/ld+json')
    facts['schema_script_count'] = len(scripts)
    remaining = MAX_BYTES // 2
    for script in scripts[:16]:
        text = script.get_text()
        if len(text) > remaining:
            facts['schema_bound_exceeded'] = True
            break
        remaining -= len(text)
        try:
            stack = [json.loads(text)]
        except (ValueError, RecursionError):
            facts['schema_parse_errors'] += 1
            continue
        while stack:
            if facts['schema_objects_seen'] >= 2048:
                facts['schema_bound_exceeded'] = True
                break
            node = stack.pop()
            facts['schema_objects_seen'] += 1
            if isinstance(node, dict):
                types = node.get('@type', [])
                types = [types] if isinstance(types, str) else types
                if isinstance(types, list):
                    if len(types) > 32:
                        facts['schema_bound_exceeded'] = True
                    for kind in types[:32]:
                        if isinstance(kind, str) and kind in _TYPES:
                            facts['known_schema_type_counts'][kind] += 1
                        else:
                            facts['other_schema_type_count'] += 1
                stack.extend(value for value in node.values() if isinstance(value, (dict, list)))
            elif isinstance(node, list):
                stack.extend(node[:2048])
                if len(node) > 2048:
                    facts['schema_bound_exceeded'] = True
        if facts['schema_bound_exceeded']:
            break
    if len(scripts) > 16:
        facts['schema_bound_exceeded'] = True
    if len(mains) == 1:
        main = mains[0]
        headings = main.select('h1')
        facts['heading_count'] = len(headings)
        facts['heading_shapes'] = [{'characters': len(n.get_text(' ', strip=True)),
            'sha256': hashlib.sha256(n.get_text(' ', strip=True).encode()).hexdigest()}
            for n in headings[:4]]
        facts['main_element_counts'] = {tag: len(main.select(tag)) for tag in
            ('article', 'table', 'video', 'audio', 'iframe')}
        facts['known_navigation_labels'] = sorted({n.get_text(' ', strip=True)
            for n in main.select('a, button, [role=tab], h2, h3')
            if n.get_text(' ', strip=True) in _LABELS})
    return facts
