"""Reviewed Taiwan News native text, not a renderer, site crawler or wire license.

The public responses stream the current article and its credit card in separate
temporary hidden React segments. Only the two observed, fully completed static
move instructions establish their relationship. No JavaScript is evaluated,
Flight data decoded, hidden/member content exposed or linked resource fetched.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urljoin, urlsplit

from bs4 import Comment, NavigableString

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


TAIWANNEWS_BODY_SELECTOR = ':scope > .taiwannews-native-body'
TAIWANNEWS_PATH_PATTERN = '^/(?:ch|zh)/news/[1-9][0-9]*$'
TAIWANNEWS_LAYOUT = 'taiwannews_completed_public_original_v1'
TAIWANNEWS_VERIFICATION = 'literal_ID_native_heading_minute_clock_credit_role_and_completed_RS_RC_segments_v1_offline_verified_20261022'
TAIWANNEWS_V2_LAYOUT = 'taiwannews_completed_public_original_v2'
TAIWANNEWS_V2_VERIFICATION = 'literal_ID_native_heading_minute_clock_credit_role_and_chronological_completed_RS_RC_segments_v2_offline_verified_20261022'
TAIWANNEWS_V3_LAYOUT = 'taiwannews_completed_public_original_v3'
TAIWANNEWS_V3_VERIFICATION = 'literal_ID_chronological_completed_segments_explicit_inline_editor_minute_and_faithful_written_text_v3_offline_verified_20261022'
# Pin old/new capture, unchanged auth and old HTTP identities before admission.
TAIWANNEWS_V2_HTTP_BINDING = (
    '85ce6bbcd70366cea5b8233c6455c9eb0b6de1a165f8d3d1884a68305c11b585',
    '73cf2cd1889574bec542863cf40c0c427b72ad9d1faa20d27c5c13700929de6b',
    'afd2e525ca6172b122816dbbe31fb4d0cde6182f0d70c44b7db4bed2f368db27',
    '85ce6bbcd70366cea5b8233c6455c9eb0b6de1a165f8d3d1884a68305c11b585',
    'afd2e525ca6172b122816dbbe31fb4d0cde6182f0d70c44b7db4bed2f368db27',
)
# Populated from the reviewed catalog's actual fingerprint, not a wildcard
# inheritance rule. The new body ABI grants no HTTP or authorization change.
TAIWANNEWS_V3_HTTP_BINDING = (
    '73cf2cd1889574bec542863cf40c0c427b72ad9d1faa20d27c5c13700929de6b',
    'e4093af203b9fe03c5d013ab475a9d2732c6a0ae4b86d8b47fa9b442fd81b0ba',
    'afd2e525ca6172b122816dbbe31fb4d0cde6182f0d70c44b7db4bed2f368db27',
    '85ce6bbcd70366cea5b8233c6455c9eb0b6de1a165f8d3d1884a68305c11b585',
    'afd2e525ca6172b122816dbbe31fb4d0cde6182f0d70c44b7db4bed2f368db27',
)
TAIWANNEWS_ORIGINAL_QUESTION = 'taiwannews_current_native_title_stream_scope_original_v1'
TAIWANNEWS_INLINE_ORIGINAL_QUESTION = 'taiwannews_legacy_literal_inline_editor_scope_original_v1'
TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION = 'taiwannews_completed_single_root_inline_editor_original_v1'

# Independently retained native responses4389317 and6360274. A different
# renderer is an unresolved source shape, never an invitation to execute it.
_RS_DEFINITION_SHA256 = '756fdd5f8b36d966ff44ac238533e54fab81348147a13479df902380c29a130b'
_RC_INSTRUCTION_SHA256 = '4d6289a4118283aefced8553df636df5dfdf56f215e569525e02c87ae6e62bab'
_SEGMENT = re.compile(r'S:[0-9]{1,2}')
_PLACEHOLDER = re.compile(r'P:[0-9]{1,2}')
_MOVE = re.compile(r'\$RS\("(S:[0-9]{1,2})","(P:[0-9]{1,2})"\)')
_MONTHS = {name: number for number, name in enumerate(
    ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'), 1)}
_CLOCK = re.compile(r'([A-Z][a-z]{2})[.] ([0-9]{1,2}), ([0-9]{4}) ([0-9]{2}):([0-9]{2})')
_THIRD_PARTY = re.compile(
    r'中央社|美通社|PR\s?Newswire|Media\s?Outreach|Globe\s?Newswire|路透(?:社)?|美聯社|'
    r'法新社|共同社|(?:本文|本篇|文章|新聞)?(?:轉載|轉自)|外電|通訊社', re.I)
_PROSE_TAGS = frozenset({'p', 'br', 'span', 'strong', 'b', 'em', 'i', 'u', 's', 'a',
                       'ul', 'ol', 'li', 'blockquote', 'h2', 'h3', 'h4', 'sub', 'sup'})


def _completed_segments(soup, *, adapter_contract_version=1, single_root=False):
    """Bounded static relation proof; do not mutate or execute source code."""
    scripts = [node for node in soup.find_all('script', src=False)
               if '$RS' in node.get_text() or '$RC' in node.get_text()]
    # Evidence18 is a separate, attested original-only question. A completed
    # root need not contain any internal $RS moves; it still needs the same
    # exact $RC, visible destination, closed boundary and execution order.
    # Do not broaden any registered article/parser ABI here.
    if (single_root and (adapter_contract_version != 3 or len(scripts) != 1)
            or not single_root and not 2 <= len(scripts) <= 9):
        raise NewsError('article_identity_not_verified')
    if hashlib.sha256(scripts[-1].get_text().encode()).hexdigest() != _RC_INSTRUCTION_SHA256:
        raise NewsError('article_identity_not_verified')
    # Use object identity, not Tag hashing (which serializes entire subtrees).
    # One linear pass proves that each literal target already exists when its
    # pinned synchronous move executes; a final-DOM graph alone cannot do so.
    positions = ({id(node): index for index, node in enumerate(soup.descendants)}
                 if adapter_contract_version >= 2 else None)
    by_id = {}
    for node in soup.find_all(id=True):
        key = node['id']
        if re.fullmatch(r'[SPB]:[0-9]+', key):
            if key in by_id:
                raise NewsError('article_identity_not_verified')
            by_id[key] = node
    root, destination = by_id.get('S:0'), by_id.get('B:0')
    if (root is None or root.name != 'div' or root.attrs != {'hidden': '', 'id': 'S:0'}
            or root.parent is not soup.body or destination is None or destination.name != 'template'
            or destination.attrs != {'id': 'B:0'} or destination.contents
            or destination.find_parent('main') is None
            or not isinstance(destination.previous_sibling, Comment)
            or str(destination.previous_sibling) != '$?'
            or scripts[-1].parent is not soup.body
            or scripts[-1].find_previous(id='S:0') is not root):
        raise NewsError('article_identity_not_verified')
    _visible(destination)
    if positions is not None and not (
            positions[id(destination)] < positions[id(scripts[-1])]
            and positions[id(root)] < positions[id(scripts[-1])]):
        raise NewsError('article_identity_not_verified')
    depth, closed = 0, False
    for index, sibling in enumerate(destination.next_siblings):
        if index >= 1024:
            break
        if isinstance(sibling, Comment):
            if str(sibling) == '/$':
                if depth == 0:
                    closed = True
                    break
                depth -= 1
            elif str(sibling) in {'$', '$?', '$!'}:
                depth += 1
    if not closed:
        raise NewsError('article_identity_not_verified')
    moves = {}
    for index, script in enumerate(scripts[:-1]):
        text = script.get_text()
        if index == 0:
            definition, separator, text = text.rpartition(';')
            if not separator or hashlib.sha256(definition.encode()).hexdigest() != _RS_DEFINITION_SHA256:
                raise NewsError('article_identity_not_verified')
        match = _MOVE.fullmatch(text)
        if not match:
            raise NewsError('article_identity_not_verified')
        segment_id, placeholder_id = match.groups()
        segment, placeholder = by_id.get(segment_id), by_id.get(placeholder_id)
        if (segment_id == 'S:0' or segment_id in moves or placeholder_id in moves.values()
                or segment is None or segment.name != 'div'
                or segment.attrs != {'hidden': '', 'id': segment_id} or segment.parent is not soup.body
                or placeholder is None or placeholder.name != 'template'
                or placeholder.attrs != {'id': placeholder_id} or placeholder.contents
                or script.parent is not soup.body or script.find_previous(id=segment_id) is not segment):
            raise NewsError('article_identity_not_verified')
        if positions is not None and not (
                positions[id(segment)] < positions[id(script)]
                and positions[id(placeholder)] < positions[id(script)]):
            raise NewsError('article_identity_not_verified')
        moves[segment_id] = placeholder_id
    if (set(by_id) != {'B:0', 'S:0', *moves, *moves.values()}
            or set(key for key in by_id if _PLACEHOLDER.fullmatch(key)) != set(moves.values())):
        raise NewsError('article_identity_not_verified')
    # Each segment must lead to the one public completed root, without a
    # cycle, unrelated placeholder or unknown hidden/conditional ancestor.
    for segment_id in moves:
        visited, current = set(), segment_id
        while current != 'S:0':
            if current in visited or current not in moves:
                raise NewsError('article_identity_not_verified')
            visited.add(current)
            target = by_id[moves[current]]
            parent = next((node for node in target.parents if _SEGMENT.fullmatch(str(node.get('id', '')))), None)
            if parent is None or parent['id'] not in {'S:0', *moves}:
                raise NewsError('article_identity_not_verified')
            current = parent['id']
    return by_id, moves


def _stream_visible(node, by_id):
    segment_found = False
    for parent in [node, *node.parents]:
        if not getattr(parent, 'attrs', None):
            continue
        key = parent.get('id')
        if _SEGMENT.fullmatch(str(key)) and by_id.get(key) is parent:
            segment_found = True
            continue  # Only the fully verified temporary wrapper is revealed.
        style = re.sub(r'\s+', '', str(parent.get('style', ''))).lower()
        if (parent.has_attr('hidden') or str(parent.get('aria-hidden', '')).lower() == 'true'
                or 'display:none' in style or 'visibility:hidden' in style
                or {'hidden', 'invisible', 'sr-only'}.intersection(parent.get('class', []))):
            raise NewsError('article_identity_not_verified')
    if not segment_found:
        raise NewsError('article_identity_not_verified')


def _native_clock(value):
    match = _CLOCK.fullmatch(value)
    try:
        if match is None:
            raise ValueError('undeclared minute format')
        month, day, year, hour, minute = match.groups()
        datetime(int(year), _MONTHS[month], int(day), int(hour), int(minute))
    except (KeyError, ValueError):
        raise NewsError('invalid_bulk_publication_clock') from None
    return value  # No seconds, offset or observation date is invented.


def _title_key(value):
    return re.sub(r'[ \t\r\n]+', ' ', value).strip()


@dataclass(frozen=True)
class _NativeParts:
    declared: str
    by_id: dict
    moves: dict
    native: object
    editor: object
    section: object
    article: object
    header: object
    heading: object


def _native_parts(soup, *, page_url, adapter_contract_version, single_root=False):
    if type(adapter_contract_version) is not int or adapter_contract_version not in (1, 2, 3):
        raise NewsError('article_identity_not_verified')
    parts = urlsplit(page_url or '')
    match = re.fullmatch(r'/zh/news/([1-9][0-9]*)', parts.path)
    if (parts.scheme != 'https' or parts.netloc != 'www.taiwannews.com.tw' or not match
            or parts.query or parts.fragment):
        raise NewsError('article_identity_not_verified')
    identifier = match[1]
    declared = _one(soup, 'head > meta[property="og:url"]').get('content')
    if declared != 'https://taiwannews.com.tw/zh/news/' + identifier:
        raise NewsError('article_identity_not_verified')
    # A bare-host declaration corroborates ID only. It is neither a transport
    # alias nor permission to generate /zh URLs from unobserved /ch redirects.
    by_id, moves = _completed_segments(soup, adapter_contract_version=adapter_contract_version,
        single_root=single_root)
    native = _one(soup, 'div[itemprop=articleBody]')
    editor = _one(native, ':scope > div.editor.app')
    if native.find_all(recursive=False) != [editor]:
        raise NewsError('article_identity_not_verified')
    section = native.parent
    article = section.parent.parent
    if section.name != 'section' or article.name != 'section' or native.parent.parent.name != 'div':
        raise NewsError('article_identity_not_verified')
    header = _one(article, ':scope > div.w-full.max-w-4xl.mx-auto')
    heading = _one(header, ':scope > div.max-w-2xl.w-full.mx-auto > h1.text-head-semibold')
    if len(soup.select('h1')) != 1:
        raise NewsError('article_identity_not_verified')
    return _NativeParts(declared, by_id, moves, native, editor, section, article, header, heading)


def _native_credit_and_prose(soup, parts, visible_clock):
    """Same static visibility, source roles and prose scope for body/evidence."""
    credit = _one(soup, 'div.flex-1 > p.text-small-regular > a[href^="/zh/journalist/"]',
                  'article_attribution_not_verified')
    name, profile = credit.get_text(' ', strip=True), credit.get('href', '')
    label = _one(credit.parent.parent, ':scope > p.text-gray-3.text-subtle-large',
                 'article_attribution_not_verified').get_text(' ', strip=True)
    if label not in {'台灣英文新聞－編輯', '台灣英文新聞－記者', '台灣英文新聞－時評'}:
        raise NewsError('article_rights_or_scope_excluded')
    if (not name or len(name) > 120 or credit.find(True) is not None
            or any(ord(char) < 32 for char in name) or not re.fullmatch(r'/zh/journalist/[1-9][0-9]*', profile)
            or credit.parent.find_all(recursive=False) != [credit]):
        raise NewsError('article_attribution_not_verified')
    card = credit.parent.parent.parent
    avatar_link = _one(card, ':scope > a', 'article_attribution_not_verified')
    if avatar_link.get('href') != profile or _one(avatar_link, ':scope > img').get('alt') != name:
        raise NewsError('article_attribution_not_verified')
    wrapper = next((node for node in card.parents if _SEGMENT.fullmatch(str(node.get('id', '')))), None)
    if wrapper is None or wrapper['id'] not in parts.moves:
        raise NewsError('article_attribution_not_verified')
    target = parts.by_id[parts.moves[wrapper['id']]]
    if not any(node is parts.header for node in target.parents):
        raise NewsError('article_attribution_not_verified')
    _native_prose_scope(parts, visible_clock, credit, card, target)
    return name, profile, label


def _native_prose_scope(parts, visible_clock, *credit_nodes, allowed_tags=_PROSE_TAGS):
    """Common whole-editor visibility/scope; not an attribution inference."""
    for node in (parts.native, parts.editor, parts.section, parts.article, parts.header,
                 parts.heading, visible_clock, *credit_nodes):
        _stream_visible(node, parts.by_id)
    for node in parts.editor.find_all(True):
        # The editor's ancestors were checked above. Inspect each descendant
        # once instead of rewalking its ancestry for every written element.
        style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
        if (node.has_attr('hidden') or str(node.get('aria-hidden', '')).lower() == 'true'
                or 'display:none' in style or 'visibility:hidden' in style
                or {'hidden', 'invisible', 'sr-only'}.intersection(node.get('class', []))
                or node.name not in allowed_tags or any(key.lower().startswith('on') for key in node.attrs)):
            raise NewsError('article_identity_not_verified')
        if (node.name == 'a' and re.match(r'([A-Za-z][A-Za-z0-9+.-]*):', node.get('href', ''))
                and not re.match(r'https?:', node.get('href', ''), re.I)):
            raise NewsError('article_identity_not_verified')
    if _THIRD_PARTY.search(parts.editor.get_text(' ', strip=True)):
        raise NewsError('article_rights_or_scope_excluded')


def taiwannews_public_original(soup, *, page_url, adapter_contract_version=1):
    parts = _native_parts(soup, page_url=page_url, adapter_contract_version=adapter_contract_version)
    title = parts.heading.get_text(' ', strip=True)
    published = _native_clock(_one(soup, 'head > meta[property="article:published_time"]').get('content', ''))
    visible_clock = _one(parts.heading.parent, ':scope > p.text-right.text-subtle-regular')
    inline_editor = (adapter_contract_version == 3
                     and visible_clock.get_text(' ', strip=True) == '台灣英文新聞－編輯 ' + published)
    og_title = _one(soup, 'head > meta[property="og:title"]').get('content', '')
    document_title = _one(soup, 'head > title').get_text(' ', strip=True)
    if (not title or (not inline_editor and visible_clock.get_text(' ', strip=True) != published)
            or _title_key(og_title) != _title_key(title + ' | Taiwan News | ' + published)
            or _title_key(document_title) != _title_key(og_title)):
        raise NewsError('article_title_not_verified')
    modifications = soup.select('head > meta[property="article:modified_time"]')
    if len(modifications) > 1:
        raise NewsError('invalid_bulk_publication_clock')
    modified = _native_clock(modifications[0].get('content', '')) if modifications else None
    if inline_editor:
        _inline_editor_scope(soup, parts, published, visible_clock)
        attribution = {'native_credit_name': None, 'native_credit_role': '台灣英文新聞－編輯',
            'native_profile_href': None, 'native_profile_id': None, 'profile_not_fetched': True,
            'source_declared_editor_group': '台灣英文新聞－編輯',
            'inline_credit_text': visible_clock.get_text(' ', strip=True),
            'attribution_kind': 'literal_organisational_editor_group_not_named_person'}
        authors = [{'@type': 'Organization', 'name': '台灣英文新聞－編輯'}]
    else:
        name, profile, label = _native_credit_and_prose(soup, parts, visible_clock)
        attribution = {'native_credit_name': name, 'native_credit_role': label, 'native_profile_href': profile,
            'native_profile_id': profile.rsplit('/', 1)[1], 'profile_not_fetched': True}
        authors = [{'name': name}]
    # Preserve only the exact native written body. The separate hero image,
    # third-party image caption, ads, support CTA and related stories remain
    # in the private raw capture, never promoted into this article's prose.
    scope = soup.new_tag('div')
    body = soup.new_tag('div', attrs={'class': 'taiwannews-native-body'})
    if inline_editor:
        # The shared legacy extractor separates text nodes. WBR and inline
        # markup do not introduce written characters or new lines. Render the
        # reviewed inline shape once so 10.<wbr>8 stays 10.8, without changing
        # any already-accepted separate-card body or the global text ABI.
        body.append(NavigableString(_inline_written_text(parts.editor)))
    else:
        body.append(deepcopy(parts.editor))
    scope.append(body)
    return {
        'headline': title, 'author': authors, 'datePublished': published, 'dateModified': modified,
        **({'attribution_status': 'source_declared_corporate_author_not_verified_person'} if inline_editor else {}),
        'author_attribution_detail_json': json.dumps({
            **attribution,
            'human_reporter_identity_verified': False, 'original_ownership_verified': False,
            'editor_or_desk_is_not_inferred_reporter': True,
            'completion_basis': 'literal_pinned_RS_moves_and_RC_completion_not_script_execution_or_render_observation',
            'completed_segment_edges': parts.moves,
        }, ensure_ascii=False),
        'published_time_basis': 'publisher_declared_minute_text_without_offset; timezone_and_historical_release_observation_not_verified',
        'article_identity_basis': ('literal_final_zh_URL_ID_native_OG_heading_clock_and_bound_completed_public_segments_v1; bare_host_not_transport_alias'
            if adapter_contract_version == 1 else
            f'literal_final_zh_URL_ID_native_OG_heading_clock_and_bound_completed_public_segments_v{adapter_contract_version}; synchronous_move_and_root_completion_order_verified; bare_host_not_transport_alias'),
        'source_title_declarations_json': json.dumps({'native_heading': title, 'OG_title': og_title,
            'document_title': document_title, 'OG_url': parts.declared}, ensure_ascii=False),
        'body_completeness': 'whole_reviewed_native_written_editor; separate_hero_media_caption_ads_support_and_related_stories_excluded; not_original_historical_version',
    }, scope


def _inline_written_text(editor):
    """O(nodes + characters), literal inline adjacency and block/BR breaks.

    Only the scope-validated written editor is traversed. This is neither
    browser rendering nor JavaScript evaluation; comments are not prose.
    """
    blocks = {'p', 'ul', 'ol', 'li', 'blockquote', 'h2', 'h3', 'h4'}
    text, stack = [], [(iter(editor.children), None)]
    while stack:
        try:
            node = next(stack[-1][0])
        except StopIteration:
            _, tag = stack.pop()
            if tag in blocks:
                text.append('\n')
            continue
        if isinstance(node, Comment):
            continue
        if isinstance(node, NavigableString):
            text.append(str(node))
        elif node.name == 'br':
            text.append('\n')
        elif node.name != 'wbr':
            if node.name in blocks:
                text.append('\n')
            stack.append((iter(node.children), node.name))
    return ''.join(text)


def taiwannews_evidence_source(source):
    """Exact current private scope; a parser/hash edit cannot rearm a GET."""
    return bool(source.provider == 'taiwannews' and source.allowed and source.request_enabled
        and source.embedded_article_layout == TAIWANNEWS_V2_LAYOUT
        and source.domains == ('www.taiwannews.com.tw',) and source.known_url_only
        and (source.data_fingerprint, source.authorization_fingerprint) == TAIWANNEWS_V2_HTTP_BINDING[1:3]
        and (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == TAIWANNEWS_V2_HTTP_BINDING[3:])


def taiwannews_resident_source(source):
    """Attested v3 body ABI only; it cannot rearm completed v16/v17 GETs."""
    return bool(len(TAIWANNEWS_V3_HTTP_BINDING) == 5
        and source.provider == 'taiwannews' and source.allowed and source.request_enabled
        and source.embedded_article_layout == TAIWANNEWS_V3_LAYOUT
        and source.domains == ('www.taiwannews.com.tw',) and source.known_url_only
        and (source.data_fingerprint, source.authorization_fingerprint) == TAIWANNEWS_V3_HTTP_BINDING[1:3]
        and (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == TAIWANNEWS_V3_HTTP_BINDING[3:])


def taiwannews_article_id(url, *, requested=False):
    parts = urlsplit(url or '')
    match = re.fullmatch(r'/(?:ch|zh)/news/([1-9][0-9]*)' if requested else r'/zh/news/([1-9][0-9]*)', parts.path)
    return (match[1] if parts.scheme == 'https' and parts.netloc == 'www.taiwannews.com.tw'
            and not parts.query and not parts.fragment and match else None)


def taiwannews_original_document(response, source, requested_url, *, evidence_contract_version=16):
    """Actual literal same-ID transport and common public envelope, once."""
    from downloader.news_fulltext_candidate_originals import MAX_ORIGINAL_BYTES
    from downloader.news_fulltext_policy import public_article_document
    requested = taiwannews_article_id(requested_url, requested=True)
    final = taiwannews_article_id(response.url)
    attested = (taiwannews_evidence_source(source) if evidence_contract_version in {16, 17} else
                taiwannews_resident_source(source) if evidence_contract_version == 18 else False)
    if (not attested or requested is None or requested != final
            or response.status != 200 or not isinstance(response.body, bytes)
            or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES or b'\x00' in response.body
            or not re.search(r'(?:text/html|application/xhtml[+]xml)', response.headers.get('content-type', ''), re.I)):
        raise NewsError('private_taiwannews_original_scope_not_verified')
    if response.url == requested_url:
        if response.redirects:
            raise NewsError('private_taiwannews_original_scope_not_verified')
    elif (requested_url != 'https://www.taiwannews.com.tw/ch/news/'+requested or len(response.redirects) != 1
            or response.redirects[0].status != 301 or response.redirects[0].from_url != requested_url
            or response.redirects[0].to_url != response.url
            or urljoin(requested_url, response.redirects[0].location) != response.url):
        raise NewsError('private_taiwannews_original_scope_not_verified')
    return public_article_document(response.body, source, page_url=response.url)


def taiwannews_original_permit(document, response, source, requested_url):
    """Independent private witness, not permission to accept mismatched titles."""
    parts = _native_parts(document, page_url=response.url, adapter_contract_version=2)
    published = _native_clock(_one(document, 'head > meta[property="article:published_time"]').get('content', ''))
    visible = _one(parts.heading.parent, ':scope > p.text-right.text-subtle-regular')
    if not parts.heading.get_text(' ', strip=True) or visible.get_text(' ', strip=True) != published:
        raise NewsError('private_taiwannews_original_scope_not_verified')
    modifications = document.select('head > meta[property="article:modified_time"]')
    if len(modifications) > 1:
        raise NewsError('invalid_bulk_publication_clock')
    if modifications:
        _native_clock(modifications[0].get('content', ''))
    _native_credit_and_prose(document, parts, visible)
    return {'contract_version': 1, 'question': TAIWANNEWS_ORIGINAL_QUESTION, 'provider': 'taiwannews',
        'url': requested_url, 'requested_url': requested_url, 'document_url': response.url,
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'current_known_public_same_ID_completed_stream_native_credit_private_original_evidence_only'}


def private_taiwannews_original_permit(response, source, requested_url):
    document = taiwannews_original_document(response, source, requested_url)
    return taiwannews_original_permit(document, response, source, requested_url)


def taiwannews_inline_original_permit(document, response, source, requested_url):
    """A separate legacy editor/clock witness, never a production body parser.

    This NEW question follows literal inline credit observed by evidence16.
    It cannot turn a bare publisher label into a person or reuse authority for
    the current separate-card layout. The normal v2 parser remains unchanged.
    """
    parts = _native_parts(document, page_url=response.url, adapter_contract_version=2)
    published = _native_clock(_one(document, 'head > meta[property="article:published_time"]').get('content', ''))
    clock = _one(parts.heading.parent, ':scope > p.text-right.text-subtle-regular')
    _inline_editor_scope(document, parts, published, clock)
    return {'contract_version': 1, 'question': TAIWANNEWS_INLINE_ORIGINAL_QUESTION, 'provider': 'taiwannews',
        'url': requested_url, 'requested_url': requested_url, 'document_url': response.url,
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'literal_legacy_inline_editor_minute_clock_same_ID_completed_stream_private_original_evidence_only'}


def _inline_editor_scope(document, parts, published, clock):
    """Exact literal group credit, never an inferred individual or wire grant."""
    if (clock.get_text(' ', strip=True) != '台灣英文新聞－編輯 ' + published
            or document.select('div.flex-1 > p.text-small-regular > a[href^="/zh/journalist/"]')):
        raise NewsError('private_taiwannews_inline_editor_not_verified')
    title = parts.heading.get_text(' ', strip=True)
    og = _one(document, 'head > meta[property="og:title"]').get('content', '')
    if (not title or _title_key(og) != _title_key(title+' | Taiwan News | '+published)
            or _title_key(_one(document, 'head > title').get_text(' ', strip=True)) != _title_key(og)):
        raise NewsError('private_taiwannews_inline_editor_not_verified')
    # A hidden child cannot contribute an invented visible editor label.
    for node in clock.find_all(True):
        style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
        if (node.name not in {'span', 'a', 'time'} or node.has_attr('hidden')
                or str(node.get('aria-hidden', '')).lower() == 'true'
                or 'display:none' in style or 'visibility:hidden' in style
                or {'hidden', 'invisible', 'sr-only'}.intersection(node.get('class', []))
                or any(key.lower().startswith('on') for key in node.attrs)):
            raise NewsError('private_taiwannews_inline_editor_not_verified')
    modifications = document.select('head > meta[property="article:modified_time"]')
    if len(modifications) > 1:
        raise NewsError('invalid_bulk_publication_clock')
    if modifications:
        _native_clock(modifications[0].get('content', ''))
    # WBR has no written characters. Reject attributes/children that would
    # require interpreting an unknown layout or hidden/scripted behaviour.
    _native_prose_scope(parts, clock, allowed_tags=_PROSE_TAGS | {'wbr'})
    if any(node.attrs or node.contents for node in parts.editor.find_all('wbr')):
        raise NewsError('article_identity_not_verified')


def private_taiwannews_inline_original_permit(response, source, requested_url):
    document = taiwannews_original_document(response, source, requested_url)
    return taiwannews_inline_original_permit(document, response, source, requested_url)


def taiwannews_single_root_original_permit(document, response, source, requested_url):
    """New completed-root witness; neither body acceptance nor a renderer.

    The evidence17 response has exactly the already pinned root-completion
    instruction and no internal moves. Prove that smaller relation without
    interpreting Flight, JavaScript, missing credit or unrelated hidden data.
    """
    parts = _native_parts(document, page_url=response.url, adapter_contract_version=3, single_root=True)
    published = _native_clock(_one(document, 'head > meta[property="article:published_time"]').get('content', ''))
    clock = _one(parts.heading.parent, ':scope > p.text-right.text-subtle-regular')
    _inline_editor_scope(document, parts, published, clock)
    return {'contract_version': 1, 'question': TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION, 'provider': 'taiwannews',
        'url': requested_url, 'requested_url': requested_url, 'document_url': response.url,
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'literal_same_ID_single_completed_root_inline_editor_minute_private_original_evidence_only'}


def private_taiwannews_single_root_original_permit(response, source, requested_url):
    document = taiwannews_original_document(response, source, requested_url, evidence_contract_version=18)
    return taiwannews_single_root_original_permit(document, response, source, requested_url)


def taiwannews_boundary_metadata(document):
    """Bounded native declarations/shape, no author names or written body."""
    from collections import Counter
    def declaration(node):
        value = node.get('content', '') if node.name == 'meta' else node.get_text(' ', strip=True)
        return ({'value': value, 'characters': len(value)} if len(value) <= 1024 else
                {'sha256': hashlib.sha256(value.encode()).hexdigest(), 'characters': len(value), 'value_omitted': True})
    fields = {
        'native_heading': 'h1', 'document_title': 'head > title', 'OG_title': 'head > meta[property="og:title"]',
        'OG_url': 'head > meta[property="og:url"]', 'published': 'head > meta[property="article:published_time"]',
        'modified': 'head > meta[property="article:modified_time"]',
        'visible_clock': 'h1.text-head-semibold ~ p.text-right.text-subtle-regular'}
    facts = {'contract_version': 1, 'declarations': {}, 'body_text_exported': False, 'author_credit_names_exported': False}
    for key, selector in fields.items():
        nodes = document.select(selector)
        facts['declarations'][key] = {'count': len(nodes), 'values': [declaration(node) for node in nodes[:2]],
                                    'additional_values_omitted': len(nodes) > 2}
    scripts = [node.get_text() for node in document.find_all('script', src=False)
               if '$RS' in node.get_text() or '$RC' in node.get_text()]
    facts['stream_scripts'] = {'count': len(scripts), 'sha256': [hashlib.sha256(value.encode()).hexdigest() for value in scripts[:16]]}
    facts['literal_moves'] = [list(match.groups()) for value in scripts[:16] for match in list(_MOVE.finditer(value))[:8]]
    editors = document.select('div[itemprop=articleBody] > div.editor.app')
    facts['editor_count'] = len(editors)
    facts['editor_tag_counts'] = []
    for editor in editors[:2]:
        counts = Counter(node.name for node in editor.find_all(True))
        bounded = {name if re.fullmatch(r'[a-z0-9:_-]{1,64}', name) else 'sha256:'+hashlib.sha256(name.encode()).hexdigest(): count
                   for name, count in sorted(counts.items())[:64]}
        facts['editor_tag_counts'].append({'counts': bounded, 'distinct_tags': len(counts),
                                          'additional_tag_values_omitted': len(counts) > 64})
    labels = document.select('div.flex-1 > p.text-gray-3.text-subtle-large')
    facts['credit_roles'] = []
    for node in labels[:8]:
        value = node.get_text(' ', strip=True)
        facts['credit_roles'].append(value if value in {'台灣英文新聞－編輯','台灣英文新聞－記者','台灣英文新聞－時評'}
            else {'unreviewed_label_sha256': hashlib.sha256(value.encode()).hexdigest(), 'characters': len(value)})
    return facts
