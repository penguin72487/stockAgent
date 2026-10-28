"""One new, body-free question for previously witnessed unresolved templates.

DOM positions, typed predicates and hashes are evidence for a later parser
decision. They are neither article text nor an author, alias or clock grant.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from urllib.parse import parse_qsl, unquote, urljoin, urlsplit

from downloader.news_fulltext_moneyweekly import (
    BODY_GEOMETRY_QUESTION, moneyweekly_body_geometry_result,
    moneyweekly_body_geometry_route, moneyweekly_body_geometry_source,
)
from downloader.news_fulltext_policy import NewsError, normalize_url

LAYOUT = 'moneyweekly_unresolved_template_metadata_v1'
QUESTION = 'moneyweekly_native_header_and_withheld_location_shape_v1'
_DIGEST = re.compile('[0-9a-f]{64}')
_TAGS = frozenset('html body main article section div span p a h1 h2 h3 h4 ul ol li table tbody tr td th time img br other'.split())
_SEGMENTS = frozenset({'ArticleData', 'Info', 'Home', 'ChangePage', 'Article', 'article', 'FN',
    'Magazine', '_Article', '理財周刊', '理周新聞', '說解財經大小事', '觀點文章', 'Error', 'NotFound', 'Index'})
_QUERY = {'Type': r'[0-9]{1,3}', 'Line': r'Include_FirstNews_[1-9][0-9]*',
    'Page': r'(?:[0-9A-F]{2}){1,1024}', 'AID': r'[1-9][0-9]*'}
_NOTICES = {
    'no_data': r'查無(?:相關)?(?:資料|文章|內容)|暫無(?:資料|文章|內容)',
    'not_found': r'文章(?:不存在|已刪除|已下架)|找不到(?:此|該)?文章',
    'members': r'本文限會員登入瀏覽|會員登入後(?:閱讀|瀏覽)',
    'read_full': r'閱讀全文|查看完整文章',
    'related': r'相關文章|相關新聞|延伸閱讀',
    'subscribe': r'立即訂閱|訂閱雜誌',
    'copyright': r'版權所有|未經授權',
}


def _sha(value):
    return hashlib.sha256(value.encode() if isinstance(value, str) else value).hexdigest()


def require_prior_template(db, source, url):
    """No new allowance without the exact closed v65 source/head evidence."""
    from downloader.news_fulltext_repairs import _bytes, _head, _source_contract
    try:
        if not moneyweekly_body_geometry_source(source) or not moneyweekly_body_geometry_route(url):
            raise ValueError
        key = _sha(_bytes([65, source.provider, _sha(url), BODY_GEOMETRY_QUESTION]))
        row = db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
        if row is None or row['state'] != 'observed':
            raise ValueError
        request, result = json.loads(row['request_json']), json.loads(row['result_json'])
        head = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        if (not moneyweekly_body_geometry_result(result, url)
                or request['target'] != _head(head)
                or request['source'] != _source_contract(db, source, url)[1]
                or _sha(_bytes(request)) != row['request_id']
                or request['candidate_fingerprint'] != source.data_fingerprint):
            raise ValueError
        parts, facts = urlsplit(url), result['html']
        magazine = (parts.path.startswith('/Magazine/Info/') and head['state'] == 'body_not_found'
            and result['status'] == 200 and facts['configured_body_count'] == 1
            and facts['author_field_count'] == facts['publication_field_count'] == 0
            and len(facts['bodies']) == 1 and 0 < facts['bodies'][0]['text_characters'] < 100
            and facts['bodies'][0]['paragraph_count'] == facts['bodies'][0]['image_count'] == 0)
        navigation = (parts.path.startswith('/Home/ChangePage/FN/') and head['state'] == 'unsupported_article_path'
            and result['status'] == 302 and result['location'] is None
            and isinstance(result['location_raw_sha256'], str))
        if not (magazine or navigation):
            raise ValueError
        return source
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError, sqlite3.OperationalError):
        raise ValueError('closed_unresolved_moneyweekly_template_required') from None


def _literal_location(value, base):
    if not isinstance(value, str) or not 0 < len(value) <= 2048:
        return None
    try:
        url = urljoin(base, value)
        parts = urlsplit(url)
        if (normalize_url(url) != url or parts.scheme not in {'http', 'https'}
                or parts.netloc != 'www.moneyweekly.com.tw' or parts.fragment):
            return None
        path = unquote(parts.path)
        if not re.fullmatch(r'/(?:ArticleData/Info/(?:Article|理財周刊|理周新聞|說解財經大小事|觀點文章)|'
                r'Magazine/Info/理財周刊|Home/ChangePage/(?:Article|理財周刊|FN))/[1-9][0-9]*/?|/_Article', path):
            return None
        pairs = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True)
        if len(pairs) > 4 or len({k for k, _ in pairs}) != len(pairs):
            return None
        if any(k not in _QUERY or not re.fullmatch(_QUERY[k], v) for k, v in pairs):
            return None
        return url
    except (ValueError, NewsError):
        return None


def _location_shape(value, url):
    value = value if isinstance(value, str) else ''
    resolved = urljoin(url, value)
    try:
        parts = urlsplit(resolved)
        raw_parts = urlsplit(value)
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        segments = []
        for segment in unquote(parts.path).split('/')[:16]:
            if not segment:
                continue
            kind = 'known' if segment in _SEGMENTS else 'decimal' if re.fullmatch('[0-9]{1,20}', segment) else 'opaque'
            segments.append(dict(kind=kind, value=segment if kind != 'opaque' else None,
                characters=len(segment), sha256=_sha(segment)))
        return dict(kind='location', header_present=bool(value), raw_characters=len(value), raw_sha256=_sha(value),
            relative_reference=not bool(raw_parts.scheme), same_publisher=parts.netloc == 'www.moneyweekly.com.tw',
            scheme=parts.scheme if parts.scheme in {'http', 'https'} else 'other',
            fragment_present=bool(parts.fragment), path_segments=segments,
            path_segments_truncated=len(unquote(parts.path).split('/')) > 16,
            query_fields=[dict(key=k if k in _QUERY else 'other', key_sha256=_sha(k),
                value_characters=len(v), value_sha256=_sha(v), known_value_shape=bool(k in _QUERY and re.fullmatch(_QUERY[k], v)))
                for k, v in pairs[:8]], query_fields_truncated=len(pairs) > 8,
            literal_url=_literal_location(value, url))
    except ValueError:
        return dict(kind='invalid_location', raw_characters=len(value), raw_sha256=_sha(value))


def _path(node):
    parts = []
    while node is not None and getattr(node, 'name', None) not in {None, '[document]'} and len(parts) < 16:
        tag = node.name if node.name in _TAGS else 'other'
        index = 1 + sum(getattr(s, 'name', None) == node.name for s in node.previous_siblings)
        parts.append(f'{tag}[{index}]')
        node = node.parent
    return '/' + '/'.join(reversed(parts))


def _node_shape(node, source):
    text = node.get_text(' ', strip=True)
    return dict(path=_path(node), tag=node.name if node.name in _TAGS else 'other',
        text_characters=len(text), text_sha256=_sha(text),
        permitted_author_exact=text in source.permitted_author_names,
        permitted_author_present=any(name in text for name in source.permitted_author_names),
        date_shape=bool(re.fullmatch(r'(?:發布日期[：:]?\s*)?[12][0-9]{3}[/-][01]?[0-9][/-][0-3]?[0-9](?:\s+[0-2][0-9]:[0-5][0-9](?::[0-5][0-9])?)?', text)),
        notice_codes=[key for key, pattern in _NOTICES.items() if re.search(pattern, text)],
        direct_element_children=sum(bool(getattr(n, 'name', None)) for n in node.children))


def template_metadata(response, source, url):
    if (not moneyweekly_body_geometry_source(source) or not moneyweekly_body_geometry_route(url)
            or not urlsplit(url).path.startswith(('/Magazine/Info/', '/Home/ChangePage/FN/'))
            or response.url != url or response.redirects):
        raise NewsError('literal_moneyweekly_template_required')
    if response.status == 404:
        raise NewsError('http_404')
    if response.status not in {200, 301, 302, 303, 307, 308}:
        raise NewsError('literal_moneyweekly_template_required')
    facts = dict(contract_version=1, evidence_contract_version=66, provider='moneyweekly', question=QUESTION,
        requested_url=url, final_url=response.url, status=response.status, redirect_count=0,
        response_bytes=len(response.body), response_sha256=_sha(response.body), current_source_not_candidate=True,
        accepted_as_article=False, body_completeness_verified=False, identity_or_alias_accepted=False,
        raw_or_names_or_body_retained=False, raw_or_body_retained=False, raw_response_retained=False,
        full_body_retained=False, destination_requested=False)
    if response.status != 200:
        facts['structure'] = _location_shape(response.headers.get('location'), url)
        return facts
    if response.headers.get('content-type', '').split(';', 1)[0].lower().strip() not in {'text/html', 'application/xhtml+xml'}:
        raise NewsError('unsupported_content_type')
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.body, source.html_parser)
    headings = soup.select('h1')
    cached = {}
    def shape(node):
        key = id(node)
        if key not in cached:
            cached[key] = _node_shape(node, source)
        return cached[key]
    scopes = []
    if len(headings) == 1:
        node = headings[0]
        for _ in range(3):
            if node is None or node.name in {'html', '[document]'}:
                break
            children = [n for n in node.children if getattr(n, 'name', None)]
            scopes.append(dict(node=shape(node), children=[shape(n) for n in children[:12]],
                children_truncated=len(children) > 12))
            node = node.parent
    bodies = soup.select(source.selectors[0])
    document_text = soup.get_text(' ', strip=True)
    candidates = headings[0].parent.select('span,time,[itemprop="author"],[itemprop="datePublished"],[rel="author"]') if len(headings) == 1 else []
    native_metadata = [value for n in candidates[:128] if (value := shape(n))['permitted_author_exact'] or value['date_shape']]
    anchors = [a for body in bodies[:2] for a in body.select('a[href]')]
    facts['structure'] = dict(kind='template', heading_count=len(headings), body_count=len(bodies),
        bodies=[shape(n) for n in bodies[:2]], bodies_truncated=len(bodies) > 2,
        scopes=scopes, global_permitted_author_present=any(name in document_text for name in source.permitted_author_names),
        native_leaf_metadata=native_metadata[:8], native_leaf_metadata_truncated=len(candidates) > 128 or len(native_metadata) > 8,
        body_links=[dict(node=shape(a), declaration=_location_shape(a.get('href'), url)) for a in anchors[:8]],
        body_links_truncated=len(anchors) > 8,
        schema_article_count=len(soup.select('[itemtype*="Article"]')), author_or_publication_accepted=False)
    return facts


def template_result(result, url):
    """Closed output schema: no arbitrary prose, URLs, fields or author names."""
    import math
    def keys(v, fields):
        return isinstance(v, dict) and set(v) == set(fields.split())
    def count(v):
        return type(v) is int and 0 <= v <= 1 << 31
    def digest(v):
        return isinstance(v, str) and bool(_DIGEST.fullmatch(v))
    def shape(v):
        return (keys(v, 'path tag text_characters text_sha256 permitted_author_exact permitted_author_present date_shape notice_codes direct_element_children')
            and isinstance(v['path'], str) and bool(re.fullmatch(r'(?:/(?:' + '|'.join(_TAGS) + r')\[[1-9][0-9]*\]){1,16}', v['path']))
            and v['tag'] in _TAGS and count(v['text_characters']) and digest(v['text_sha256'])
            and all(type(v[k]) is bool for k in ('permitted_author_exact', 'permitted_author_present', 'date_shape'))
            and isinstance(v['notice_codes'], list) and len(v['notice_codes']) <= len(_NOTICES)
            and all(k in _NOTICES for k in v['notice_codes']) and count(v['direct_element_children']))
    def location(v):
        if v.get('kind') == 'invalid_location':
            return keys(v, 'kind raw_characters raw_sha256') and count(v['raw_characters']) and digest(v['raw_sha256'])
        return (keys(v, 'kind header_present raw_characters raw_sha256 relative_reference same_publisher scheme fragment_present path_segments path_segments_truncated query_fields query_fields_truncated literal_url')
            and v['kind'] == 'location' and v['scheme'] in {'http', 'https', 'other'}
            and all(type(v[k]) is bool for k in ('header_present', 'relative_reference', 'same_publisher', 'fragment_present', 'path_segments_truncated', 'query_fields_truncated'))
            and count(v['raw_characters']) and digest(v['raw_sha256'])
            and (v['literal_url'] is None or isinstance(v['literal_url'], str) and _literal_location(v['literal_url'], url) == v['literal_url'])
            and isinstance(v['path_segments'], list) and len(v['path_segments']) <= 16
            and all(keys(n, 'kind value characters sha256') and count(n['characters']) and digest(n['sha256'])
                and (n['kind'] == 'opaque' and n['value'] is None or n['kind'] == 'known' and n['value'] in _SEGMENTS
                    or n['kind'] == 'decimal' and isinstance(n['value'], str) and re.fullmatch('[0-9]{1,20}', n['value'])) for n in v['path_segments'])
            and isinstance(v['query_fields'], list) and len(v['query_fields']) <= 8
            and all(keys(n, 'key key_sha256 value_characters value_sha256 known_value_shape') and n['key'] in {*_QUERY, 'other'}
                and digest(n['key_sha256']) and count(n['value_characters']) and digest(n['value_sha256']) and type(n['known_value_shape']) is bool for n in v['query_fields']))
    try:
        false = 'accepted_as_article body_completeness_verified identity_or_alias_accepted raw_or_names_or_body_retained raw_or_body_retained raw_response_retained full_body_retained destination_requested'.split()
        fields = 'contract_version evidence_contract_version provider question requested_url final_url status redirect_count response_bytes response_sha256 current_source_not_candidate structure ' + ' '.join(false)
        if (not keys({k:v for k,v in result.items() if k != 'timing'}, fields)
                or result['contract_version'] != 1 or result['evidence_contract_version'] != 66
                or result['provider'] != 'moneyweekly' or result['question'] != QUESTION
                or not moneyweekly_body_geometry_route(url) or result['requested_url'] != url or result['final_url'] != url
                or not urlsplit(url).path.startswith(('/Magazine/Info/', '/Home/ChangePage/FN/'))
                or result['redirect_count'] != 0 or result['current_source_not_candidate'] is not True
                or any(result[k] is not False for k in false) or not count(result['response_bytes'])
                or not digest(result['response_sha256'])):
            return False
        if 'timing' in result and (not keys(result['timing'], 'http_seconds parse_seconds scope_seconds total_seconds')
                or any(type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 3600
                    for value in result['timing'].values())):
            return False
        v = result['structure']
        if result['status'] == 200:
            return (keys(v, 'kind heading_count body_count bodies bodies_truncated scopes global_permitted_author_present native_leaf_metadata native_leaf_metadata_truncated body_links body_links_truncated schema_article_count author_or_publication_accepted')
                and v['kind'] == 'template' and count(v['heading_count']) and count(v['body_count'])
                and isinstance(v['bodies'], list) and len(v['bodies']) == min(2, v['body_count']) and all(shape(n) for n in v['bodies'])
                and v['bodies_truncated'] is (v['body_count'] > 2) and isinstance(v['scopes'], list) and len(v['scopes']) <= 3
                and all(keys(n, 'node children children_truncated') and shape(n['node']) and isinstance(n['children'], list)
                    and len(n['children']) <= 12 and all(shape(child) for child in n['children']) and type(n['children_truncated']) is bool for n in v['scopes'])
                and type(v['global_permitted_author_present']) is bool and count(v['schema_article_count'])
                and isinstance(v['native_leaf_metadata'], list) and len(v['native_leaf_metadata']) <= 8
                and all(shape(n) for n in v['native_leaf_metadata']) and type(v['native_leaf_metadata_truncated']) is bool
                and isinstance(v['body_links'], list) and len(v['body_links']) <= 8
                and all(keys(n, 'node declaration') and shape(n['node']) and location(n['declaration']) for n in v['body_links'])
                and type(v['body_links_truncated']) is bool
                and v['author_or_publication_accepted'] is False)
        if result['status'] not in {301, 302, 303, 307, 308}:
            return False
        return location(v)
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError):
        return False
