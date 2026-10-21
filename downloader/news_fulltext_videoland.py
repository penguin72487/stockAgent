"""Videoland's visible written originals; no HTTP, discovery or media fetch.

The native body includes text captions omitted from articleBody. Match its
prose against the same-URL free schema, but retain the entire visible body.
Publisher reporter labels are declarations, not verified personal identities.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from itertools import islice
import json
import re
from urllib.parse import parse_qs, urlsplit

from bs4.element import Comment, NavigableString

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


VIDEOLAND_ORIGIN = 'https://news.videoland.com.tw'
VIDEOLAND_POLICY_EVIDENCE_URL = VIDEOLAND_ORIGIN + '/article/06609930-538e-4271-927f-c06c4a1c9d51.html'
VIDEOLAND_PATH_PATTERN = r'^/article/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}[.]html$'
VIDEOLAND_BODY_SELECTOR = ':scope > .videoland-native-body'
_COPYRIGHT = '未經同意禁止任何形式之轉載 © COPYRIGHT VIDEOLAND INC. ALL RIGHTS RESERVED.'
_BRAND = '緯來新聞網'
_REPORTER_CREDIT = re.compile(r'記者(?P<name>[\u3400-\u9fff]{2,12})／[\u3400-\u9fff]{2,12}報導')
_JSON_SYNTAX_ERRORS = {
    'Invalid control character at': 'unescaped_control_character',
    'Unterminated string starting at': 'unterminated_string',
    'Expecting property name enclosed in double quotes': 'expected_quoted_property',
    "Expecting ':' delimiter": 'expected_colon',
    "Expecting ',' delimiter": 'expected_comma',
    'Expecting value': 'expected_value',
    'Extra data': 'extra_data',
    'Invalid \\escape': 'invalid_escape',
    'Invalid \\uXXXX escape': 'invalid_unicode_escape',
}
_JSON_CONTEXT_FIELDS = frozenset({'description', 'articleBody', 'headline', 'name',
    'datePublished', 'dateModified', 'url', '@id', '@type'})
_JSON_CONTEXT_MAX_TOKENS = 4096
_JSON_CONTEXT_MAX_DEPTH = 32


def _json_control_context(raw, error, *, include_string_start=False):
    """Locate the first bad string in a bounded, strict prefix; retain no values.

    This is diagnostic only: it neither repairs JSON nor decodes its remainder.
    A preceding @type describes this owning object's valid prefix, not a parsed
    NewsArticle declaration (a later duplicate key could still change it).
    """
    facts = {'contract_version': 2 if include_string_start else 1, 'proved': False, 'reason': 'unsupported_error',
        'role': None, 'field': None, 'owning_object_newsarticle_type_in_valid_prefix': False,
        'tokens_examined': 0, 'nesting_depth': 0,
        'maximum_tokens': _JSON_CONTEXT_MAX_TOKENS, 'maximum_depth': _JSON_CONTEXT_MAX_DEPTH}
    if (not isinstance(raw, str) or len(raw) > 1 << 20
            or not isinstance(error, json.JSONDecodeError) or error.doc != raw
            or error.msg != 'Invalid control character at'
            or not 0 <= error.pos < len(raw) or ord(raw[error.pos]) >= 32):
        return facts
    decoder, stack, cursor, root_consumed = json.JSONDecoder(), [], 0, False

    def consume_value(value=None):
        nonlocal root_consumed
        if not stack:
            if root_consumed:
                return False
            root_consumed = True
            return True
        frame = stack[-1]
        if frame['kind'] == 'object':
            if frame['state'] != 'value':
                return False
            if frame['key'] == '@type':
                frame['newsarticle'] = value == 'NewsArticle'
        elif frame['state'] not in {'value', 'value_or_end'}:
            return False
        frame['state'] = 'separator'
        return True

    while cursor < len(raw):
        while cursor < len(raw) and raw[cursor] in ' \t\r\n':
            cursor += 1
        if cursor >= len(raw):
            break
        if facts['tokens_examined'] >= _JSON_CONTEXT_MAX_TOKENS:
            facts['reason'] = 'prefix_token_limit'
            return facts
        facts['tokens_examined'] += 1
        frame = stack[-1] if stack else None
        facts['nesting_depth'] = len(stack)
        char = raw[cursor]
        if char in '{[':
            if len(stack) >= _JSON_CONTEXT_MAX_DEPTH:
                facts['reason'] = 'prefix_depth_limit'
                return facts
            if not consume_value():
                break
            stack.append({'kind': 'object' if char == '{' else 'array',
                'state': 'key_or_end' if char == '{' else 'value_or_end',
                'key': None, 'newsarticle': False})
        elif char in '}]':
            kind, empty = ('object', 'key_or_end') if char == '}' else ('array', 'value_or_end')
            if not frame or frame['kind'] != kind or frame['state'] not in {empty, 'separator'}:
                break
            stack.pop()
        elif char == ':':
            if not frame or frame['kind'] != 'object' or frame['state'] != 'colon':
                break
            frame['state'] = 'value'
        elif char == ',':
            if not frame or frame['state'] != 'separator':
                break
            frame['state'] = 'key' if frame['kind'] == 'object' else 'value'
        else:
            is_key = bool(frame and frame['kind'] == 'object' and frame['state'] in {'key', 'key_or_end'})
            is_value = (not frame and not root_consumed or frame and (
                frame['state'] == 'value' or frame['kind'] == 'array' and frame['state'] == 'value_or_end'))
            if not (is_key and char == '"' or is_value):
                break
            try:
                value, end = decoder.raw_decode(raw, cursor)
            except json.JSONDecodeError as current:
                if (char == '"' and current.pos == error.pos
                        and current.msg == 'Invalid control character at'):
                    facts.update(proved=True, reason='matched_first_string_error',
                        role='object_key' if is_key else 'object_value' if frame and frame['kind'] == 'object'
                             else 'array_value' if frame else 'root_value',
                        field=frame['key'] if frame and frame['kind'] == 'object' and not is_key else None,
                        owning_object_newsarticle_type_in_valid_prefix=bool(
                            frame and frame['kind'] == 'object' and frame['newsarticle']))
                    if include_string_start:
                        facts['string_start_position'] = cursor
                else:
                    facts['reason'] = 'different_prefix_decode_error'
                return facts
            except (ValueError, TypeError, RecursionError):
                facts['reason'] = 'prefix_decode_failure'
                return facts
            if is_key:
                frame.update(state='colon', key=value if value in _JSON_CONTEXT_FIELDS else 'other')
            elif not consume_value(value):
                break
            cursor = end
            continue
        cursor += 1
    facts['reason'] = 'prefix_grammar_or_position_mismatch'
    return facts


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def _reject_json_constant(value):
    raise ValueError('nonfinite_json_constant')


def _description_lf_repair(raw, error):
    """Re-escape at most16 literal LF values in one observed root description.

    Raw HTML is never changed. Strict-decode the entire repaired script and
    independently decode the original description's valid segments, joining
    with the same LF values. Unknown fields, other controls, duplicate keys,
    malformed escapes and additional syntax errors cannot use this repair.
    """
    proof = {'contract_version': 1, 'candidate_decoded': False, 'reason': 'unsupported_context',
        'field': 'description', 'literal_lf_count': 0, 'maximum_literal_lf': 16,
        'description_value_preserved': False, 'only_root_description_syntax_changed': False,
        'raw_HTML_modified': False, 'accepted_as_article': False}
    context = _json_control_context(raw, error, include_string_start=True)
    if not (context['proved'] and context['role'] == 'object_value'
            and context['field'] == 'description' and context['nesting_depth'] == 1
            and context['owning_object_newsarticle_type_in_valid_prefix'] and raw[error.pos] == '\n'):
        return None, proof
    start, positions = context['string_start_position'], []
    cursor, end = start+1, None
    while cursor < len(raw):
        char = raw[cursor]
        if char == '\\':
            cursor += 2  # Strict decoding below still validates every escape.
            continue
        if char == '"':
            end = cursor
            break
        if ord(char) < 32:
            if char != '\n':
                proof.update(reason='other_description_control', literal_lf_count=len(positions))
                return None, proof
            if len(positions) == proof['maximum_literal_lf']:
                proof.update(reason='description_control_count_limit', literal_lf_count=len(positions)+1)
                return None, proof
            positions.append(cursor)
        cursor += 1
    proof['literal_lf_count'] = len(positions)
    if end is None or not positions or positions[0] != error.pos:
        proof['reason'] = 'description_string_boundary_mismatch'
        return None, proof
    pieces, previous = [], 0
    for position in positions:
        pieces.extend((raw[previous:position], '\\n'))
        previous = position+1
    pieces.append(raw[previous:])
    repaired = ''.join(pieces)
    try:
        decoded = json.loads(repaired, object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant)
        # This oracle is confined to this exact string, never strict=False.
        segments, previous = [], start+1
        for position in [*positions, end]:
            segments.append(json.loads('"'+raw[previous:position]+'"'))
            previous = position+1
    except (ValueError, TypeError, RecursionError):
        proof['reason'] = 'repaired_script_or_original_segments_not_strict_JSON'
        return None, proof
    if (not isinstance(decoded, dict) or decoded.get('@type') != 'NewsArticle'
            or not isinstance(decoded.get('description'), str)
            or decoded['description'] != '\n'.join(segments)):
        proof['reason'] = 'root_or_description_value_mismatch'
        return None, proof
    proof.update(candidate_decoded=True, reason='root_description_LF_values_preserved',
        description_value_preserved=True, only_root_description_syntax_changed=True,
        script_character_positions=positions)
    return decoded, proof


def _decode_schema(raw, *, allow_description_lf=False):
    try:
        return json.loads(raw), None
    except json.JSONDecodeError as error:
        if not allow_description_lf:
            raise
        decoded, proof = _description_lf_repair(raw, error)
        if decoded is None:
            raise error from None
        return decoded, proof


def videoland_article_id(url, *, requested=False):
    """Literal supported URL only; never derive an alias or request URL."""
    if not isinstance(url, str) or type(requested) is not bool:
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'news.videoland.com.tw'
            or parts.query or parts.fragment or not re.fullmatch(VIDEOLAND_PATH_PATTERN, parts.path)):
        return None
    return parts.path[len('/article/'):-len('.html')]


def _native_search_query(href):
    """Read a declared link's one name; never follow it or rewrite a URL."""
    if (not isinstance(href, str) or href != href.strip()
            or any(ord(char) < 32 or ord(char) == 127 for char in href)):
        return None
    try:
        parts = urlsplit(href)
        if (parts.scheme != 'https' or parts.netloc != 'news.videoland.com.tw'
                or parts.path != '/search' or parts.fragment):
            return None
        query = parse_qs(parts.query, keep_blank_values=True, strict_parsing=True,
                         max_num_fields=1, errors='strict')
        return query['q'][0] if set(query) == {'q'} and len(query['q']) == 1 and query['q'][0] else None
    except (ValueError, UnicodeError):
        return None


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if getattr(parent, 'attrs', None) and set(parent.get('class', [])).intersection(
                {'hidden', 'd-none', 'sr-only', 'screen-reader-text'}):
            raise NewsError('article_identity_not_verified')


def _meta(soup, name):
    return _one(soup, f'head > meta[property="{name}"][content]').get('content')


def _declared_clock(soup, declared, field, meta_field):
    """Require literal, aware schema/meta agreement; never fill a date or zone."""
    value = declared.get(field)
    try:
        if (not isinstance(value, str) or not re.fullmatch(
                r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})', value)):
            raise ValueError
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError
    except (TypeError, ValueError):
        raise NewsError('invalid_bulk_publication_clock') from None
    if _meta(soup, meta_field) != value:
        raise NewsError('invalid_bulk_publication_clock')
    return value, parsed


def _clock(soup, header, declared, field, native_id, meta_field):
    visible = _one(header, f'#{native_id}', 'invalid_bulk_publication_clock')
    _native_visible(visible)
    text = visible.get_text(' ', strip=True)
    value, parsed = _declared_clock(soup, declared, field, meta_field)
    if (not re.fullmatch(r'[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}', text)
            or parsed.strftime('%Y/%m/%d %H:%M') != text):
        raise NewsError('invalid_bulk_publication_clock')
    return value, parsed


def _article_clocks(soup, header, declared, *, allow_date_only=False):
    """The observed date-only template is not evidence of a midnight release.

    The candidate keeps the actual schema/meta seconds and explicit +08 zone,
    agrees with the visible unpadded calendar date, and separately records that
    there is no visible update time. Legacy minute templates stay strict.
    This is not a credit/originality grant or historical PIT reconstruction.
    """
    date_only = False
    if allow_date_only:
        visible = _one(header, '#NewsRealDate', 'invalid_bulk_publication_clock')
        _native_visible(visible)
        text = visible.get_text(' ', strip=True)
        date_only = bool(re.fullmatch(r'[0-9]{4}/[1-9][0-9]?/[1-9][0-9]?', text))
    if date_only:
        for node in visible.find_all(True):
            _native_visible(node)
        published, published_clock = _declared_clock(soup, declared, 'datePublished', 'article:published_time')
        modified, modified_clock = _declared_clock(soup, declared, 'dateModified', 'article:modified_time')
        if (published_clock.utcoffset() != timedelta(hours=8)
                or modified_clock.utcoffset() != timedelta(hours=8)
                or text != f'{published_clock.year}/{published_clock.month}/{published_clock.day}'):
            raise NewsError('invalid_bulk_publication_clock')
        updates = header.select('#NewsModDate')
        if updates:
            _clock(soup, header, declared, 'dateModified', 'NewsModDate', 'article:modified_time')
        basis = ('same_response_visible_release_calendar_day_matches_schema; '
                 'schema_and_meta_release_update_seconds_and_explicit_UTC_plus_08_agree; '
                 + ('visible_update_minute_matches; ' if updates else 'native_visible_update_absent; ')
                 + 'declared_precision_preserved_no_midnight_or_update_invention; not_observed_historical_PIT')
    else:
        published, published_clock = _clock(soup, header, declared, 'datePublished', 'NewsRealDate', 'article:published_time')
        modified, modified_clock = _clock(soup, header, declared, 'dateModified', 'NewsModDate', 'article:modified_time')
        basis = ('same_response_native_release_and_update_minutes_match_schema_and_meta; '
                 'declared_seconds_and_timezone_preserved; not_observed_historical_PIT')
    if modified_clock < published_clock:
        raise NewsError('invalid_bulk_publication_clock')
    return published, modified, basis


def videoland_validation_metadata(soup, *, page_url):
    """Finite structural/credit equalities from this envelope, never prose.

    These are diagnostic facts, not acceptance or attribution proof. Keep
    native/schema spellings private by reporting comparisons rather than
    unvalidated publisher fields, markup, search queries or names.
    """
    headers = soup.select('main header')
    bylines = headers[0].select('a#author') if len(headers) == 1 else []
    bodies = soup.select('main article > .news-content > #NewsBody')
    scripts = soup.find_all('script', type='application/ld+json', limit=17)
    schemas, schema_error = [], len(scripts) > 16
    schema_diagnostics = {'contract_version': 2, 'scripts_seen': len(scripts),
        'script_count_is_lower_bound': len(scripts) == 17, 'scripts_examined': 0,
        'scripts_parsed': 0, 'objects_examined': 0, 'complete': False,
        'first_error': {'kind': 'script_count_limit', 'maximum': 16} if schema_error else None,
        'raw_json_or_exception_message_retained': False, 'syntax_repair_attempted': False}
    if not schema_error:
        for script_index, script in enumerate(scripts):
            schema_diagnostics['scripts_examined'] += 1
            raw = script.get_text()
            if len(raw) > 1 << 20:
                schema_error = True
                schema_diagnostics['first_error'] = {'kind': 'script_size_limit',
                    'script_index': script_index, 'characters': len(raw), 'maximum': 1 << 20}
                break
            phase = 'json_decode'
            try:
                decoded = json.loads(raw)
                schema_diagnostics['scripts_parsed'] += 1
                phase = 'object_walk'
                nodes = list(islice(_objects(decoded), 65))
            except json.JSONDecodeError as error:
                # Never serialize str(error), error.msg or its source document:
                # names/body snippets can be embedded in exception messages.
                schema_error = True
                kind = _JSON_SYNTAX_ERRORS.get(error.msg, 'other_json_decode_error')
                control = (ord(raw[error.pos]) if kind == 'unescaped_control_character'
                           and 0 <= error.pos < len(raw) and ord(raw[error.pos]) < 32 else None)
                schema_diagnostics['first_error'] = {'kind': kind, 'script_index': script_index,
                    'characters': len(raw), 'line': error.lineno, 'column': error.colno,
                    'position': error.pos, 'control_codepoint': control}
                if control is not None:
                    schema_diagnostics['first_error']['string_context'] = _json_control_context(raw, error)
                break
            except (ValueError, TypeError, RecursionError) as error:
                schema_error = True
                exception_kind = ('recursion' if isinstance(error, RecursionError) else
                                  'type' if isinstance(error, TypeError) else 'value')
                schema_diagnostics['first_error'] = {'kind': phase + '_' + exception_kind,
                    'script_index': script_index, 'characters': len(raw)}
                break
            schema_diagnostics['objects_examined'] += len(nodes)
            if len(nodes) > 64:
                schema_error = True
                schema_diagnostics['first_error'] = {'kind': 'object_count_limit',
                    'script_index': script_index, 'maximum': 64}
                break
            schemas.extend(node for node in nodes if node.get('@type') == 'NewsArticle')
    schema_diagnostics['complete'] = not schema_error
    declared = schemas[0] if len(schemas) == 1 else {}
    credit = bylines[0].get_text(' ', strip=True) if len(bylines) == 1 else ''
    contiguous_credit = bylines[0].get_text('', strip=False).strip() if len(bylines) == 1 else ''
    author = declared.get('author')
    candidates = author if isinstance(author, list) else [author]
    name = candidates[0].get('name') if len(candidates) == 1 and isinstance(candidates[0], dict) else None
    compact = lambda value: re.sub(r'\s+', '', value) if isinstance(value, str) else None
    href = bylines[0].get('href') if len(bylines) == 1 else None
    reporter = _REPORTER_CREDIT.fullmatch(credit)
    reporter_name = reporter.group('name') if reporter else None
    contiguous_reporter = _REPORTER_CREDIT.fullmatch(contiguous_credit)
    compact_reporter = _REPORTER_CREDIT.fullmatch(compact(credit) or '')
    href_shape = None
    search_query = _native_search_query(href)
    href_credit_query_agrees = bool(credit and search_query == credit)
    if isinstance(href, str):
        if href == VIDEOLAND_ORIGIN + '/search?q=' + credit:
            href_shape = 'exact_native_credit_search'
        elif href.startswith(VIDEOLAND_ORIGIN + '/search?'):
            href_shape = 'different_same_origin_search'
        elif href.startswith('/search?'):
            href_shape = 'relative_search'
        else:
            href_shape = 'other_or_missing'
    body = bodies[0] if len(bodies) == 1 else None
    prose = (''.join(str(node) for node in body.descendants if isinstance(node, NavigableString)
        and not isinstance(node, Comment) and node.find_parent('figcaption') is None) if body else '')
    prefix = compact(prose) or ''
    title_nodes = headers[0].select('h1 > #NewsTitle') if len(headers) == 1 else []
    title = title_nodes[0].get_text(' ', strip=True) if len(title_nodes) == 1 else None
    def meta_value(field):
        nodes = soup.select(f'head > meta[property="{field}"][content]')
        return nodes[0].get('content') if len(nodes) == 1 else None
    canonicals = soup.select('head > link[rel~="canonical"][href]')
    document_titles = soup.select('head > title')
    document_title = document_titles[0].get_text(' ', strip=True) if len(document_titles) == 1 else None
    og_title, schema_title = meta_value('og:title'), declared.get('headline')
    identity = {'canonical_count': len(canonicals), 'native_title_count': len(title_nodes),
        'canonical_url_agrees': len(canonicals) == 1 and canonicals[0].get('href') == page_url,
        'og_url_agrees': meta_value('og:url') == page_url,
        'schema_url_agrees': declared.get('url') == page_url,
        'schema_main_entity_agrees': declared.get('mainEntityOfPage') == {'@type':'WebPage','@id':page_url},
        'native_title_equals_og': bool(title and title == og_title),
        'native_title_equals_schema': bool(title and title == schema_title),
        'native_title_compact_equals_og': bool(title and compact(title) == compact(og_title)),
        'native_title_compact_equals_schema': bool(title and compact(title) == compact(schema_title)),
        'native_title_plus_brand_equals_document_title': bool(title and document_title == title + '｜' + _BRAND),
        'free_news_article_as_declared': declared.get('isAccessibleForFree') is True}
    active_tags = ('script','style','template','noscript','iframe','video','audio','object','embed','canvas','form','button','input','textarea','select','svg')
    return {'contract_version': 5, 'literal_route_supported': videoland_article_id(page_url) is not None,
        'header_count': len(headers), 'byline_count': len(bylines), 'body_count': len(bodies),
        'schema_count': len(schemas), 'schema_bounded_or_json_error': schema_error,
        'schema_processing': schema_diagnostics,
        'declared_author_container': 'list' if isinstance(author, list) else 'object' if isinstance(author, dict) else 'other',
        'single_declared_author': len(candidates) == 1,
        'declared_author_type_person': len(candidates) == 1 and isinstance(candidates[0], dict) and candidates[0].get('@type') == 'Person',
        'declared_author_is_publisher_organization': len(candidates) == 1 and isinstance(candidates[0], dict)
            and candidates[0].get('@type') == 'Organization' and name == _BRAND,
        'schema_author_name_equals_native_credit': name == credit,
        'schema_author_name_compact_equals_native_credit': bool(credit and compact(name) == compact(credit)),
        'native_credit_is_reporter_signature': reporter is not None,
        'native_byline_contiguous_equals_joined_text': bool(credit and credit == contiguous_credit),
        'native_contiguous_credit_is_reporter_signature': contiguous_reporter is not None,
        'native_compact_credit_is_reporter_signature': compact_reporter is not None,
        'schema_name_matches_contiguous_reporter_component': bool(contiguous_reporter and name == contiguous_reporter.group('name')),
        'search_query_matches_contiguous_reporter_component': bool(contiguous_reporter and search_query == contiguous_reporter.group('name')),
        'schema_name_matches_compact_reporter_component': bool(compact_reporter and name == compact_reporter.group('name')),
        'search_query_matches_compact_reporter_component': bool(compact_reporter and search_query == compact_reporter.group('name')),
        'schema_author_name_equals_native_reporter_component': bool(reporter_name and name == reporter_name),
        'native_byline_same_origin_search_query_matches_reporter_component': bool(reporter_name and search_query == reporter_name),
        'native_byline_same_origin_search_query_matches_schema_name': bool(name and search_query == name),
        'native_byline_href_shape': href_shape,
        'native_byline_same_origin_search_query_agrees': href_credit_query_agrees,
        'native_prose_without_captions_equals_schema': bool(prose and compact(prose) == compact(declared.get('articleBody'))),
        'native_brand_bracket': '【】' if prefix.startswith('【緯來新聞網】') else '〖〗' if prefix.startswith('〖緯來新聞網〗') else None,
        'native_caption_count': len(body.select('figcaption')) if body else None,
        'native_body_active_tags': {tag:len(body.find_all(tag)) for tag in active_tags if body and body.find(tag)},
        'native_identity_comparisons': identity,
        'body_text_retained': False, 'author_names_or_hrefs_retained': False,
        'accepted_as_article': False}


def videoland_description_repair_metadata(soup, *, page_url):
    """Same-envelope candidate facts, without exporting values or admitting it."""
    facts = {'contract_version': 1, 'candidate_metadata_only': True,
        'accepted_as_article': False, 'raw_JSON_or_names_or_body_retained': False,
        'body_source_changed': False, 'source_requests': 0,
        'candidate_schema_decoded': False, 'reason': 'requires_single_bounded_script'}
    scripts = soup.find_all('script', type='application/ld+json', limit=2)
    if len(scripts) != 1 or len(scripts[0].get_text()) > 1 << 20:
        return facts
    raw = scripts[0].get_text()
    try:
        json.loads(raw)
    except json.JSONDecodeError as error:
        declared, proof = _description_lf_repair(raw, error)
        facts['syntax_candidate'] = proof
        if declared is None:
            facts['reason'] = proof['reason']
            return facts
    except (ValueError, TypeError, RecursionError):
        facts['reason'] = 'strict_decode_resource_or_type_failure'
        return facts
    else:
        facts['reason'] = 'strict_JSON_needs_no_description_repair'
        return facts
    facts.update(candidate_schema_decoded=True, reason='description_LF_candidate_decoded')
    headers, bodies = soup.select('main header'), soup.select('main article > .news-content > #NewsBody')
    header = headers[0] if len(headers) == 1 else None
    native = bodies[0] if len(bodies) == 1 else None
    author = declared.get('author')
    name = author.get('name') if isinstance(author, dict) else None
    author_type = author.get('@type') if isinstance(author, dict) else None
    facts.update(schema_author_type=author_type if isinstance(author_type, str) and author_type in {'Person', 'Organization'} else 'other',
        schema_author_is_single_object=isinstance(author, dict),
        schema_author_name_nonempty=isinstance(name, str) and bool(name.strip()),
        schema_author_is_publisher_organization=isinstance(author, dict)
            and author_type == 'Organization' and name == _BRAND,
        schema_body_field_present='articleBody' in declared,
        schema_publisher_agrees=isinstance(declared.get('publisher'), dict)
            and declared['publisher'].get('@type') == 'Organization' and declared['publisher'].get('name') == _BRAND,
        free_as_declared=declared.get('isAccessibleForFree') is True,
        schema_literal_URL_agrees=declared.get('url') == page_url,
        schema_main_entity_agrees=declared.get('mainEntityOfPage') == {'@type': 'WebPage', '@id': page_url})
    # Retain finite shape/equality facts only, including potential non-anchor
    # native credits. A matching node is discovery, not an attribution grant.
    nodes = header.find_all(['a', 'span', 'p', 'div'], limit=65) if header else []
    matches = []
    for node in nodes[:64]:
        if not isinstance(name, str) or not name or node.get_text(' ', strip=True) != name:
            continue
        try:
            _native_visible(node)
            visible = True
        except NewsError:
            visible = False
        matches.append({'tag': node.name,
            'id_kind': node.get('id') if node.get('id') in {'author', 'NewsAuthor', 'byline', 'reporter'} else 'other',
            'visible': visible, 'has_href': node.has_attr('href'),
            'native_credit_search_query_agrees': _native_search_query(node.get('href')) == name})
    facts.update(native_header_count=len(headers), native_body_count=len(bodies),
        native_header_nodes_examined=min(64, len(nodes)), native_header_scan_truncated=len(nodes) > 64,
        native_header_name_matches_count=len(matches), native_header_name_matches=matches[:8],
        native_header_name_match_details_truncated=len(matches) > 8)
    if header:
        headings = header.select('h1 > #NewsTitle')
        title = headings[0].get_text(' ', strip=True) if len(headings) == 1 else None
        facts['native_title_matches_schema'] = bool(title and title == declared.get('headline'))
        try:
            _, published = _clock(soup, header, declared, 'datePublished', 'NewsRealDate', 'article:published_time')
            _, modified = _clock(soup, header, declared, 'dateModified', 'NewsModDate', 'article:modified_time')
        except NewsError as error:
            facts['native_release_update_clock_check'] = error.code
        else:
            facts['native_release_update_clock_check'] = 'matches' if modified >= published else 'revision_precedes_release'
    if native:
        prose = ''.join(str(node) for node in native.descendants if isinstance(node, NavigableString)
            and not isinstance(node, Comment) and node.find_parent('figcaption') is None)
        compact = re.sub(r'\s+', '', prose)
        facts['native_brand_prefix_matches'] = compact.startswith('【'+_BRAND+'】')
        facts['native_prose_matches_declared_body'] = (isinstance(declared.get('articleBody'), str)
            and compact == re.sub(r'\s+', '', declared['articleBody']))
    return facts


def _clock_shape(value):
    """Finite observed date fields; unknown source text is never exported.

    Absent seconds/fractions/offsets stay None, not an invented zero. Parsed
    objects below are for equality diagnosis only, never an admission clock.
    """
    facts = {'type': 'missing' if value is None else 'string' if isinstance(value, str) else 'other',
             'format': 'unknown', 'characters': len(value) if isinstance(value, str) else None}
    if not isinstance(value, str) or not 1 <= len(value) <= 96:
        return facts, None
    try:
        encoded = value.encode()
    except UnicodeEncodeError:
        facts['format'] = 'invalid_unicode_scalar'
        return facts, None
    facts['value_sha256'] = hashlib.sha256(encoded).hexdigest()
    iso = re.fullmatch(r'([0-9]{4}-[0-9]{2}-[0-9]{2})([T ])([0-9]{2}:[0-9]{2})'
                      r'(?::([0-9]{2})(?:[.]([0-9]{1,6}))?)?(Z|[+-][0-9]{2}:?[0-9]{2})?', value)
    local = re.fullmatch(r'([0-9]{4})([/年-])([0-9]{2})([/月-])([0-9]{2})(日?)[ ]'
                        r'([0-9]{2}):([0-9]{2})(?::([0-9]{2}))?', value)
    try:
        if iso:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            seconds, fraction = iso.group(4), iso.group(5)
            facts['format'] = ('iso_T' if iso.group(2) == 'T' else 'dash_space')
        elif local and ((local.group(2), local.group(4), local.group(6)) in
                        {('/', '/', ''), ('-', '-', ''), ('年', '月', '日')}):
            parsed = datetime(int(local.group(1)), int(local.group(3)), int(local.group(5)),
                              int(local.group(7)), int(local.group(8)), int(local.group(9) or 0))
            seconds, fraction = local.group(9), None
            facts['format'] = {'/': 'slash_space', '-': 'dash_space', '年': 'chinese_ymd_space'}[local.group(2)]
        elif re.fullmatch(r'[0-9]{4}[-/][0-9]{2}[-/][0-9]{2}', value) and value[4] == value[7]:
            parsed = datetime.strptime(value, '%Y-%m-%d' if value[4] == '-' else '%Y/%m/%d')
            facts.update(format='date_only', fields=[parsed.year, parsed.month, parsed.day, None, None, None, None],
                         precision='day', offset_seconds=None, fractional_digits=None)
            return facts, None
        else:
            return facts, None
    except ValueError:
        facts['format'] = 'invalid_calendar_or_offset'
        return facts, None
    facts.update(fields=[parsed.year, parsed.month, parsed.day, parsed.hour, parsed.minute,
                         parsed.second if seconds is not None else None,
                         parsed.microsecond if fraction is not None else None],
                 precision='fraction' if fraction is not None else 'second' if seconds is not None else 'minute',
                 fractional_digits=len(fraction) if fraction is not None else None,
                 offset_seconds=parsed.utcoffset().total_seconds() if parsed.utcoffset() is not None else None)
    return facts, parsed


def videoland_credit_clock_metadata(soup, *, page_url):
    """Reviewed evidence v2: non-anchor credit and each exact clock boundary.

    No body/name/title/raw string, repair, inferred release or source request.
    UTC+08 equality is a labelled comparison hypothesis, not a timezone grant.
    """
    facts = {'contract_version': 1, 'accepted_as_article': False,
             'raw_names_or_body_retained': False, 'source_requests': 0, 'reason': 'requires_bounded_native_schema'}
    scripts = soup.find_all('script', type='application/ld+json', limit=2)
    if len(scripts) != 1 or len(scripts[0].get_text()) > 1 << 20:
        return facts
    try:
        declared, _ = _decode_schema(scripts[0].get_text(), allow_description_lf=True)
    except (ValueError, TypeError, RecursionError):
        return facts
    if not isinstance(declared, dict) or declared.get('@type') != 'NewsArticle':
        return facts
    facts.update(reason='native_credit_clock_shapes', literal_URL_agrees=declared.get('url') == page_url)
    headers = soup.select('main header')
    if len(headers) != 1:
        facts['reason'] = 'requires_single_native_header'
        return facts
    header = headers[0]
    nodes = header.select('#author')
    author = declared.get('author')
    name = author.get('name') if isinstance(author, dict) else None
    credit = {'node_count': len(nodes), 'schema_is_single_Person': isinstance(author, dict)
              and author.get('@type') == 'Person', 'schema_name_nonempty': isinstance(name, str) and bool(name.strip())}
    if len(nodes) == 1:
        node = nodes[0]
        try:
            _native_visible(node)
            visible = True
        except NewsError:
            visible = False
        text = node.get_text(' ', strip=True)
        reporter = _REPORTER_CREDIT.fullmatch(text)
        prefixes = ('記者', '編輯', '編譯', '數位中心', '新聞中心', '綜合', '文／', '文/', '攝影')
        credit.update(tag=node.name if node.name in {'a', 'span', 'p', 'div'} else 'other', visible=visible,
            has_href=node.has_attr('href'), native_equals_schema_name=text == name,
            native_characters=len(text), reporter_signature=reporter is not None,
            reporter_component_equals_schema=bool(reporter and reporter.group('name') == name),
            plain_han_name=bool(re.fullmatch(r'[\u3400-\u9fff]{2,12}', text)),
            prefix_class=next((prefix for prefix in prefixes if text.startswith(prefix)), 'other'),
            nonoriginal_credit_token=any(token in text for token in ('中央社', '路透', '美聯社', '法新社', '轉載')))
    facts['credit'] = credit
    clocks = {}
    for field, native_id, meta_name in (('datePublished', 'NewsRealDate', 'article:published_time'),
                                      ('dateModified', 'NewsModDate', 'article:modified_time')):
        nodes = header.select('#'+native_id)
        metas = soup.select('head > meta[property="'+meta_name+'"][content]')
        text = nodes[0].get_text(' ', strip=True) if len(nodes) == 1 else None
        value = declared.get(field)
        meta = metas[0].get('content') if len(metas) == 1 else None
        native_shape, native_clock = _clock_shape(text)
        schema_shape, schema_clock = _clock_shape(value)
        meta_shape, meta_clock = _clock_shape(meta)
        row = {'native_count': len(nodes), 'meta_count': len(metas),
               'native': native_shape, 'schema': schema_shape, 'meta': meta_shape,
               'schema_literal_equals_meta': isinstance(value, str) and value == meta,
               'native_visible': None, 'schema_wall_equals_native_at_native_precision': None,
               'schema_at_UTC_plus_08_equals_native_at_native_precision': None,
               'schema_meta_same_aware_instant': None}
        if len(nodes) == 1:
            try:
                _native_visible(nodes[0])
                row['native_visible'] = True
            except NewsError:
                row['native_visible'] = False
        if native_clock is not None and schema_clock is not None:
            width = 5 if native_shape['precision'] == 'minute' else 6 if native_shape['precision'] == 'second' else 7
            observed = native_shape['fields'][:width]
            row['schema_wall_equals_native_at_native_precision'] = schema_shape['fields'][:width] == observed
            if schema_clock.utcoffset() is not None:
                try:
                    local = schema_clock.astimezone(timezone(timedelta(hours=8)))
                except (OverflowError, ValueError):
                    row['UTC_plus_08_conversion_out_of_range'] = True
                else:
                    local_fields = [local.year, local.month, local.day, local.hour, local.minute, local.second, local.microsecond]
                    row['schema_at_UTC_plus_08_equals_native_at_native_precision'] = local_fields[:width] == observed
        if schema_clock is not None and meta_clock is not None and all(
                clock.utcoffset() is not None for clock in (schema_clock, meta_clock)):
            row['schema_meta_same_aware_instant'] = schema_clock == meta_clock
        clocks[field] = row
    facts['clocks'] = clocks
    return facts


def videoland_visible_article(soup, *, page_url, adapter_contract_version=1):
    # v5 is an offline engineering candidate, deliberately not registered in
    # production PolicyRegistry: non-anchor/plain names still need review.
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3, 4, 5}:
        raise NewsError('article_identity_not_verified')
    identity = videoland_article_id(page_url)
    if identity is None:
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup, 'head > link[rel~="canonical"][href]')
    if (canonical.get('href') != page_url or _meta(soup, 'og:url') != page_url
            or _meta(soup, 'og:site_name') != _BRAND or _meta(soup, 'og:type') != 'article'):
        raise NewsError('article_identity_not_verified')

    header = _one(soup, 'main header')
    heading = _one(header, 'h1 > #NewsTitle')
    native = _one(soup, 'main article > .news-content > #NewsBody', 'body_not_found')
    article = native.find_parent('article')
    if header.parent is not article.parent:
        raise NewsError('article_identity_not_verified')
    for node in (header, heading, native):
        _native_visible(node)
    title = heading.get_text(' ', strip=True)
    if (not title or _one(header, 'h1').get_text(' ', strip=True) != title
            or _meta(soup, 'og:title') != title
            or _one(soup, 'head > title').get_text(' ', strip=True) != title + '｜' + _BRAND):
        raise NewsError('article_identity_not_verified')

    scripts = soup.find_all('script', type='application/ld+json', limit=17)
    if len(scripts) > 16:
        raise NewsError('article_identity_not_verified')
    declarations, schema_repairs = [], []
    for script in scripts:
        raw = script.get_text()
        if len(raw) > 1 << 20:
            raise NewsError('article_identity_not_verified')
        try:
            decoded, repair = _decode_schema(raw, allow_description_lf=adapter_contract_version >= 4)
            if repair is not None:
                schema_repairs.append(repair)
            nodes = list(islice(_objects(decoded), 65))
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
        if len(nodes) > 64:
            raise NewsError('article_identity_not_verified')
        for node in nodes:
            if node.get('@type') == 'NewsArticle':
                declarations.append(node)
    if len(declarations) != 1:
        raise NewsError('article_identity_not_verified')
    declared = declarations[0]
    if declared.get('isAccessibleForFree') is not True:
        raise NewsError('paywall_or_login')
    if (declared.get('url') != page_url
            or declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or declared.get('headline') != title
            or not isinstance(declared.get('publisher'), dict)
            or declared['publisher'].get('@type') != 'Organization'
            or declared['publisher'].get('name') != _BRAND):
        raise NewsError('article_identity_not_verified')

    byline = _one(header, 'a#author', 'article_attribution_not_verified')
    _native_visible(byline)
    credit = byline.get_text(' ', strip=True)
    author = declared.get('author')
    if not isinstance(author, dict) or author.get('@type') != 'Person':
        raise NewsError('article_attribution_not_verified')
    reporter = _REPORTER_CREDIT.fullmatch(credit)
    complete_credit = (author.get('name') == credit
                       and byline.get('href') == VIDEOLAND_ORIGIN + '/search?q=' + credit)
    name_component = bool(adapter_contract_version >= 2 and reporter
        and author.get('name') == reporter.group('name')
        and _native_search_query(byline.get('href')) == reporter.group('name'))
    if not (complete_credit or name_component):
        raise NewsError('article_attribution_not_verified')
    if (reporter is None
            or any(value in credit for value in ('中央社', '路透', '美聯社', '法新社', '轉載'))):
        raise NewsError('article_rights_or_scope_excluded')
    published, modified, time_basis = _article_clocks(soup, header, declared,
        allow_date_only=adapter_contract_version >= 5)

    # Compare only native prose against its schema, retaining all captions in
    # the stored body. Never promote the shorter schema to "complete" text.
    if native.select('script,style,template,noscript,iframe,video,audio,object,embed,canvas,form,button,input,textarea,select,svg'):
        raise NewsError('body_not_found')
    prose = []
    for node in native.descendants:
        if isinstance(node, Comment):
            continue
        if isinstance(node, NavigableString):
            if node.find_parent('figcaption') is None:
                prose.append(str(node))
        elif getattr(node, 'name', None):
            _native_visible(node)
    text = re.sub(r'\s+', '', ''.join(prose))
    schema_body = declared.get('articleBody')
    # articleBody is optional metadata, not the source of the native written
    # body. The observed v3 envelope omits that field entirely while retaining
    # the same literal-URL/title/reporter/clock/free-access/native-container
    # evidence. Absence is distinct from null, a summary, or a contradiction.
    schema_body_absent = adapter_contract_version >= 3 and 'articleBody' not in declared
    if (not text.startswith('【' + _BRAND + '】')
            or not re.search(r'[\u3400-\u9fffA-Za-z0-9]', text[len('【' + _BRAND + '】'):])
            or (not schema_body_absent and (not isinstance(schema_body, str)
                or re.sub(r'\s+', '', schema_body) != text))):
        raise NewsError('body_not_found')
    copyrights = [node for node in soup.select('footer p')
                  if re.sub(r'\s+', ' ', node.get_text(' ', strip=True)) == _COPYRIGHT]
    if len(copyrights) != 1:
        raise NewsError('article_rights_not_verified')
    _native_visible(copyrights[0])
    images = native.select('img')
    notices = [
        {'kind': 'publisher_copyright', 'text': copyrights[0].get_text(' ', strip=True),
         'open_license_or_bulk_permission_inferred': False},
        {'kind': 'native_text_captions_preserved', 'count': len(native.select('figcaption')),
         'schema_article_body_omits_native_captions': True},
        {'kind': 'unfetched_inline_images', 'count': len(images), 'media_requested': False,
         'image_ownership_inferred': False},
    ]
    notices.extend({'kind': 'schema_description_literal_LF_syntax_repair',
                    **{key: value for key, value in repair.items() if key != 'accepted_as_article'},
                    'syntax_repair_does_not_grant_attribution_or_rights': True}
                   for repair in schema_repairs)
    if schema_body_absent:
        notices.append({'kind': 'schema_article_body_optional_field_absent',
            'body_source': 'whole_visible_native_article_container',
            'schema_description_or_summary_used_as_body': False})
    for image in images:
        image.decompose()
    scope = soup.new_tag('div')
    normalized = soup.new_tag('div', attrs={'class': 'videoland-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    scope.append(normalized)
    return {
        'headline': title, 'author': [author], 'datePublished': published, 'dateModified': modified,
        'published_time_basis': time_basis,
        'article_identity_basis': 'literal_https_request_response_no_alias; '
            'canonical_og_schema_url_heading_native_reporter_and_free_scope_agree; '
            + ('whole_visible_native_body_with_captions; optional_schema_articleBody_absent_as_declared'
               if schema_body_absent else 'whole_native_prose_matches_schema_without_removing_stored_captions'),
        'author_attribution_detail_json': json.dumps({'native_credit': credit,
            'role': 'publisher_declared_reporter', 'schema_type_as_declared': 'Person',
            'author_search_link_not_profile_or_independent_identity': True,
            'reporter_real_identity_independently_verified': False,
            **({'credit_representation': 'reporter_name_with_native_signature' if name_component else 'complete_reporter_signature',
                'schema_author_as_declared': author,
                'native_reporter_component_matches_schema_and_search_query': name_component}
               if adapter_contract_version >= 2 else {})}, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'body_completeness': 'whole_native_written_prose_and_text_captions_once; '
            'source_brand_and_copyright_notice_retained; images_unfetched_not_transcribed; '
            'current_source_response_not_original_historical_version',
    }, scope
