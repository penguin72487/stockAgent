"""Reviewed publisher layouts; declared source identity, clocks and rights.

These are extensions to the canonical extractor, not another HTTP pipeline.
Hydration is used only for the visible public article's metadata, never to get
hidden/paid content or to turn a related article/summary into a full body.
"""
from __future__ import annotations

import html
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, NamedTuple
from urllib.parse import SplitResult, unquote, urljoin, urlsplit

from downloader.news_fulltext_policy import NewsError, _select_nodes
from downloader.news_fulltext_metrics import CNYES_NATIVE_BODY_FAILURE_STAGES


_FLIGHT_ROW = re.compile(rb'([0-9a-f]{1,16}):')
_FLIGHT_LENGTH = re.compile(rb'[0-9a-f]{1,8},')

# Exact native completion instruction independently retained from the public
# mobile responses 5456724 and 5456507. Unknown renderer versions stay gaps.
# React completeBoundary moves children out of the temporary hidden wrapper;
# this is a static declaration check, never JavaScript execution or hydration.
_CNYES_FIZZ_COMPLETION_SHA256 = '4d6289a4118283aefced8553df636df5dfdf56f215e569525e02c87ae6e62bab'
# Pinned native v1 owner/capture contract: only its mobile identity failures
# may be rechecked once after the completed-stream repair, never refusals.
CNYES_MOBILE_CANONICAL_V1_CAPTURE_FINGERPRINT = '01ac5937abb27a3ddb04c80ccc4f6815e8505378a869ac5798851f05ed9f5571'
CNYES_MOBILE_CANONICAL_V2_CAPTURE_FINGERPRINT = '8da80406c37c0af51a44bd28c299a93c59b2f4f324fb430383fef0620e74a978'
CNYES_MOBILE_CANONICAL_V3_CAPTURE_FINGERPRINT = 'bb696cb74968c6497c8dba13f2eb940fe97b818c8b8cdead29a474b39aecc007'
CNYES_MOBILE_CANONICAL_V4_CAPTURE_FINGERPRINT = '03401a6231773f01fd2752748bb7b4670ad7cfff25a73ec1b6f1a9ad6204746d'
CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT = 'df72420e23365658d4662b108c74af29c56fcb18b09dfc0eeb0c084c9986a653'
# Verified against the retained v6 rollout receipt and the native-policy
# fingerprint reconstruction. Earlier short-body fingerprints are not v6.
CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT = '2c69e752ee3632e83fa4bde770b6c833b03ce4985596f4eea51cfc68a6fe3c05'
CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT = '7fd7e72cab5f57dae18dc2e67bb1a6210f95396cdea27ed376c95069f9891384'
# The v15 production capture contract before the independently retained
# 4955510 empty-signature repair. Only its terminal parser failures are due.
CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT = 'ac0ca35513f4d69bf658f6c127833b8274e2bc634f4875695981ef6ac556c824'
CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT = 'ee5e2a8af38ee38fd93dfe2eb99e064252ea1677fab395b693b97d7969289777'


def cnyes_visible_headline_matches(visible, declared):
    """Match get_text(strip=True)'s outer-space view, preserving source text.

    Actual mobile 5419221 declares a trailing U+00A0 in BOTH current data and
    JSON-LD; the visible h1's strip=True view omits it. Internal spaces, hidden
    controls, HTML/entity rewrites and different words are not normalized.
    This only corroborates identity; it never supplies or edits the title.
    """
    return isinstance(visible, str) and isinstance(declared, str) and visible == declared.strip()


def _cnyes_completed_stream_wrapper(soup, article):
    """Recognize only the observed B:0 -> S:0 completed public render.

    Arbitrary hidden nodes, error/truncated instructions, ambiguous IDs and an
    invisible destination are not a rendered article. No DOM mutation, script
    evaluation, external renderer or new network request is needed.
    """
    from bs4 import Comment
    from downloader.news_fulltext_public_originals import _visible

    if article.find_parent('div', id='S:0') is None:
        return None  # Ordinary non-streamed pages need no whole-document scan.
    wrappers, destinations = soup.find_all(id='S:0'), soup.find_all(id='B:0')
    if len(wrappers) != 1 or len(destinations) != 1:
        return None
    wrapper, destination = wrappers[0], destinations[0]
    if (wrapper.name != 'div' or wrapper.attrs != {'hidden': '', 'id': 'S:0'}
            or not any(parent is wrapper for parent in article.parents)
            or wrapper.parent.name != 'body' or destination.name != 'template'
            or destination.attrs != {'id': 'B:0'} or destination.contents
            or wrapper.find_previous(id='B:0') is not destination
            or not isinstance(destination.previous_sibling, Comment)
            or str(destination.previous_sibling) != '$?'):
        return None
    _visible(destination)
    # Native Flight frames and other completed segments can arrive between
    # this wrapper and its reveal. Immediate adjacency was a synthetic-only
    # assumption; require real document order, one exact instruction and IDs.
    completions = [script for script in soup.find_all('script', src=False)
                   if '$RC' in script.get_text()]
    if len(completions) != 1:
        return None
    instruction = completions[0]
    if (instruction.parent is not wrapper.parent or instruction.find_previous(id='S:0') is not wrapper
            or hashlib.sha256(instruction.get_text().encode()).hexdigest() != _CNYES_FIZZ_COMPLETION_SHA256):
        return None
    _visible(instruction)
    # The observed instruction must have an actual bounded, closed Suspense
    # fallback at the visible destination, not just look like a reveal call.
    depth = 0
    for index, sibling in enumerate(destination.next_siblings):
        if index >= 1024:
            return None
        if isinstance(sibling, Comment):
            marker = str(sibling)
            if marker == '/$':
                if depth == 0:
                    return wrapper
                depth -= 1
            elif marker in {'$', '$?', '$!'}:
                depth += 1
    return None


def _cnyes_visible_after_completed_stream(node, revealed_wrapper):
    """Only the verified transient wrapper is revealed; descendants are not."""
    from downloader.news_fulltext_public_originals import _visible

    if revealed_wrapper is None:
        return _visible(node)
    for parent in [node, *node.parents]:
        if parent is revealed_wrapper or not getattr(parent, 'attrs', None):
            continue
        style = re.sub(r'\s+', '', str(parent.get('style', ''))).lower()
        if (parent.has_attr('hidden') or str(parent.get('aria-hidden', '')).lower() == 'true'
                or 'display:none' in style or 'visibility:hidden' in style):
            raise NewsError('article_identity_not_verified')


def _cnyes_flight_frames(fragments):
    """Read bounded framing, not JavaScript or a general Flight interpreter.

    React text rows use UTF-8 byte lengths, not lines or Python characters.
    Skipping a complete length-framed row also prevents JSON-looking text
    inside it from becoming an unrelated article declaration. Only T strings
    can corroborate a current content reference; binary/module rows cannot.
    https://github.com/facebook/react/blob/main/packages/react-client/src/ReactFlightClient.js
    """
    try:
        stream = ''.join(fragments).encode('utf-8')
    except UnicodeError:
        return None
    if len(stream) > 8 << 20:
        return None
    models, texts, offset, rows = [], {}, 0, 0
    while offset < len(stream):
        row = _FLIGHT_ROW.match(stream, offset)
        if row is None or rows >= 4096:
            return None
        marker, offset = row[1].decode('ascii'), row.end()
        rows += 1
        if offset >= len(stream):
            return None
        tag = stream[offset]
        if tag in b'TAOobUSsLlGgMmV':
            length = _FLIGHT_LENGTH.match(stream, offset + 1)
            if length is None:
                return None
            start = length.end()
            end = start + int(length[0][:-1], 16)
            if end > len(stream):
                return None
            if tag == ord('T'):
                value = stream[start:end]
                if marker in texts and texts[marker] != value:
                    return None
                texts[marker] = value
            offset = end  # Text rows need not end with a newline.
        else:
            end = stream.find(b'\n', offset)
            if end < 0:
                return None
            if tag in b'[{':
                models.append(stream[offset:end])
            offset = end + 1
    return models, texts


def _cnyes_current_article_data(soup, news_id, *, verify_video=False, verify_author=False):
    """Read corroborating declarations only; never use them to supply a body."""
    from downloader.news_fulltext_policy import _objects
    fragments = []
    for script in soup.find_all('script', src=False):
        push = re.fullmatch(r'self[.]__next_f[.]push\((.+)\);?', script.get_text(), re.S)
        if not push:
            continue
        try:
            chunk = json.loads(push[1])
        except (ValueError, TypeError):
            continue
        if (isinstance(chunk, list) and len(chunk) == 2 and type(chunk[0]) is int
                and chunk[0] == 1 and isinstance(chunk[1], str)):
            fragments.append(chunk[1])
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('ambiguous_duplicate_key')
            value[key] = item
        return value

    frames = _cnyes_flight_frames(fragments)
    if frames is None:
        return None
    models, texts = frames
    candidates = []
    for payload in models:
        try:
            nodes = _objects(json.loads(payload, object_pairs_hook=unique_object))
        except (ValueError, TypeError, RecursionError):
            continue
        for node in nodes:
            data = node.get('data')
            if (isinstance(data, dict) and type(data.get('newsId')) is int
                    and data['newsId'] == news_id and 'content' in data):
                candidates.append(data)
    if not candidates:
        return None
    # A repeated identical render is harmless; conflicting current-article
    # declarations cannot be resolved by choosing the first or latest object.
    fields = ('newsId', 'title', 'content', 'payment', 'publishAt', 'url')
    if verify_video:
        fields += ('video',)
    if verify_author:
        fields += ('author',)
    data = candidates[0]
    if any(any(type(other.get(k)) is not type(data.get(k)) or other.get(k) != data.get(k)
               for k in fields) for other in candidates[1:]):
        return None
    content = data.get('content')
    if isinstance(content, str) and re.fullmatch(r'\$[0-9a-f]{1,16}', content):
        referenced = texts.get(content[1:])
        if referenced is None:
            return None
        try:
            data = {**data, 'content': referenced.decode('utf-8')}
        except UnicodeError:
            return None
    return data


def _cnyes_visible_article_scope(soup):
    """Bind the header to the single body, not SSR document arrival order.

    Native React streamed pages put the article in S:0 after a sidebar loading
    h1. Only the article ancestor of the public body is an identity witness.
    Reject ambiguous/nested article headers and unrelated page-wide matches.
    """
    containers = _select_nodes(soup, '#article-container')
    if len(containers) != 1:
        return None
    scope = containers[0].find_parent('article')
    if scope is None:
        return None
    headers = scope.find_all('h1')
    if len(headers) != 1:
        return None
    return headers[0], containers[0]


def _cnyes_table_grids(scope, source):
    """Preserve cell boundaries: 22|184 must not equal 2|2184."""
    from downloader.news_fulltext_policy import _plain_text_node

    result = []
    for table in scope.find_all('table'):
        if table.find('table') is not None:
            return None
        result.append([[re.sub(r'\s+', '', _plain_text_node(cell, source.exclude_selectors,
                                                         parser=source.html_parser))
                        for cell in row.find_all(['th', 'td'], recursive=False)]
                       for row in table.find_all('tr')])
    return result


def _cnyes_nested_table_witness(scope, source):
    """Compare a native table forest without flattening nested cell boundaries.

    The retained 5117411 has the same complete visible/current nested table.
    A linear, nonmutating event walk retains row groups, each cell's text and
    span/header attributes, mixed outer-cell text and nested-table positions.
    Presentation-only inline wrappers do not move text across cell events.
    Unknown/hidden/excluded markup and malformed structural ownership fail
    closed. This does not relax the existing simple-grid/short-body contract.
    """
    from bs4 import CData, Comment, NavigableString, Tag
    from downloader.news_fulltext_policy import _native_exclusions

    exclusions = _native_exclusions((*source.exclude_selectors,
                                    '[id^="div-gpt-ad-cnyes_news_article_"]'))
    if exclusions is None:
        return None
    classes, ids, prefixes = exclusions
    parents = {'table': {None, 'td', 'th'}, 'thead': {'table'}, 'tbody': {'table'},
               'tfoot': {'table'}, 'tr': {'table', 'thead', 'tbody', 'tfoot'},
               'td': {'tr'}, 'th': {'tr'}}
    inline = {'p', 'div', 'span', 'strong', 'em', 'b', 'i', 'u', 's', 'br', 'a',
              'sup', 'sub', 'small', 'ul', 'ol', 'li'}
    tokens, text, structural, table_rows = [], [], [], []
    nested = 0

    def flush():
        value = re.sub(r'\s+', '', ''.join(text))
        if value:
            tokens.append(('text', value))
        text.clear()

    # Iterators rather than Python recursion or whole-subtree text reads keep
    # work O(DOM + text bytes), including deeply nested input.
    stack = [(scope, iter(scope.children), False)]
    while stack:
        node, children, marked = stack[-1]
        try:
            child = next(children)
        except StopIteration:
            stack.pop()
            if marked:
                flush()
                frame = structural.pop()
                if frame['tag'] == 'table':
                    if table_rows.pop() == 0:
                        return None
                elif frame['tag'] in ('tr', 'thead', 'tbody', 'tfoot') and frame['children'] == 0:
                    return None
                tokens.append(('close', frame['tag']))
            continue
        if isinstance(child, Tag):
            kind = child.name
            parent = structural[-1]['tag'] if structural else None
            inside = parent is not None or kind == 'table'
            if inside:
                style = re.sub(r'\s+', '', str(child.get('style', ''))).lower()
                node_id = child.get('id')
                if (child.has_attr('hidden') or child.has_attr('inert')
                        or str(child.get('aria-hidden', '')).lower() == 'true'
                        or 'display:none' in style or 'visibility:hidden' in style
                        or node_id in ids or classes.intersection(child.get('class', []))
                        or isinstance(node_id, str) and any(node_id.startswith(p) for p in prefixes)):
                    return None
            marked = kind in parents
            if marked:
                if parent not in parents[kind]:
                    return None
                flush()
                if structural:
                    structural[-1]['children'] += 1
                if kind == 'table':
                    nested += int(parent is not None)
                    table_rows.append(0)
                elif kind == 'tr':
                    table_rows[-1] += 1
                semantic = tuple((key, child.get(key)) for key in
                                 ('rowspan', 'colspan', 'headers', 'scope', 'abbr', 'summary'))
                tokens.append(('open', kind, semantic))
                structural.append({'tag': kind, 'children': 0})
            elif inside and (kind not in inline or parent not in ('td', 'th')):
                return None
            stack.append((child, iter(child.children), marked))
        elif structural and isinstance(child, Comment):
            return None
        elif structural and type(child) in (NavigableString, CData):
            if structural[-1]['tag'] not in ('td', 'th') and child.strip():
                return None
            text.append(str(child))
    return tokens if nested else None


def _cnyes_remove_leading_paragraph_markers(scope):
    from bs4 import NavigableString, Comment

    removed = 0
    for paragraph in scope.find_all('p'):
        first = paragraph.contents[0] if paragraph.contents else None
        if (isinstance(first, NavigableString) and not isinstance(first, Comment)
                and str(first).startswith('\ufeff') and not str(first).startswith('\ufeff\ufeff')):
            first.replace_with(str(first)[1:])
            removed += 1
    return removed


def _cnyes_remove_verified_em_paragraph_markers(scope, full_content, source):
    """Compare the observed 5157359 p > em leading marker without editing body.

    Only the exact single-text, attribute-free emphasized paragraph is covered.
    Its unique current-content paragraph must have identical markup and text.
    In-word/repeated markers, other wrappers, hidden or ambiguous structures
    are not generalized into formatting; every source word/number is retained.
    Parse current markup only if a candidate exists, once per comparison view.
    """
    from bs4 import BeautifulSoup, Comment, NavigableString, Tag
    from collections import Counter

    current_paragraphs = None
    removed = 0
    for paragraph in scope.find_all('p'):
        if paragraph.attrs or len(paragraph.contents) != 1:
            continue
        emphasized = paragraph.contents[0]
        if (not isinstance(emphasized, Tag) or emphasized.name != 'em'
                or emphasized.attrs or len(emphasized.contents) != 1):
            continue
        first = emphasized.contents[0]
        if (not isinstance(first, NavigableString) or isinstance(first, Comment)
                or not str(first).startswith('\ufeff') or str(first).startswith('\ufeff\ufeff')
                or len(str(first)) <= 1):
            continue
        if current_paragraphs is None:
            current = BeautifulSoup(html.unescape(full_content), source.html_parser)
            current_paragraphs = Counter(str(node) for node in current.find_all('p'))
        original = str(paragraph)
        if current_paragraphs[original] != 1:
            continue
        first.replace_with(str(first)[1:])
        removed += 1
    return removed


def _cnyes_remove_verified_anchor_markers(scope, full_content, source):
    """Comparison only for the observed native list-link headline marker.

    Public responses 5415376, 5418903 and 5276212 and their current free
    content preserve one U+FEFF after an exact observed headline prefix;
    JSON-LD omits it. Require the identical unique current-content link and
    text. Index current links once, rather than searching the DOM per link.
    Do not strip arbitrary
    in-word markers, repeated markers, numeric text or the retained body.
    """
    from bs4 import BeautifulSoup, NavigableString, Comment
    from collections import defaultdict

    current_links = None
    removed = 0
    prefixes = ('盤中速報 - \ufeff', '盤後速報 - \ufeff', '鉅亨速報 - Factset 最新調查：\ufeff')
    for anchor in scope.find_all('a', href=True):
        if (anchor.parent.name != 'li' or anchor.parent.parent.name != 'ul'
                or len(anchor.contents) != 1):
            continue
        text = anchor.contents[0]
        prefix = next((value for value in prefixes if str(text).startswith(value)), None)
        if (not isinstance(text, NavigableString) or isinstance(text, Comment)
                or prefix is None or not str(text)[len(prefix):]
                or str(text)[len(prefix):].startswith('\ufeff')):
            continue
        parts = urlsplit(anchor['href'])
        if (parts.scheme != 'https' or parts.netloc != 'news.cnyes.com'
                or re.fullmatch(r'/news/id/[0-9]+', parts.path) is None
                or parts.query or parts.fragment):
            continue
        if current_links is None:
            current = BeautifulSoup(html.unescape(full_content), source.html_parser)
            current_links = defaultdict(list)
            for candidate in current.find_all('a', href=True):
                current_links[candidate['href']].append(candidate)
        matches = current_links.get(anchor['href'], ())
        if (len(matches) != 1 or len(matches[0].contents) != 1
                or matches[0].get_text() != str(text)
                or matches[0].parent.name != 'li' or matches[0].parent.parent.name != 'ul'):
            continue
        text.replace_with(str(text).replace(prefix, prefix[:-1], 1))
        removed += 1
    return removed


def _cnyes_remove_verified_factset_markers(scope, full_content, source):
    """Compare the observed FactSet company-prefix U+FEFF, not real words.

    Public responses 5432092 and 5437568 retain one marker after their
    literal FactSet/rating prefix, before the company and stock link. Their
    JSON-LD omits it. Require one identical current-content paragraph and
    stock link. Only the comparison clone changes; body/raw/numbers do not.
    Index current paragraphs lazily once, not once per candidate paragraph.
    """
    from bs4 import BeautifulSoup, NavigableString, Comment

    prefix = re.compile(r'^(根據FactSet最新調查，共[0-9]+位分析師，對|'
                        r'綜合評級 - 共有[0-9]+位分析師給予)\ufeff([^()\ufeff]+)\($')
    current_paragraphs = None
    removed = 0
    for paragraph in scope.find_all('p'):
        if len(paragraph.contents) < 2:
            continue
        first, anchor = paragraph.contents[:2]
        if not isinstance(first, NavigableString) or isinstance(first, Comment):
            continue
        match = prefix.fullmatch(str(first))
        if not match or getattr(anchor, 'name', None) != 'a' or not anchor.get_text():
            continue
        parts = urlsplit(anchor.get('href', ''))
        if (parts.scheme != 'https' or parts.netloc != 'www.cnyes.com'
                or not re.fullmatch(r'/twstock/[0-9]{4,6}', parts.path)
                or parts.query or parts.fragment):
            continue
        if current_paragraphs is None:
            current = BeautifulSoup(html.unescape(full_content), source.html_parser)
            current_paragraphs = {}
            for candidate in current.find_all('p'):
                candidate_first = candidate.contents[0] if candidate.contents else None
                if isinstance(candidate_first, NavigableString) and not isinstance(candidate_first, Comment):
                    key = (str(candidate_first), candidate.get_text())
                    current_paragraphs.setdefault(key, []).append(candidate)
        matches = current_paragraphs.get((str(first), paragraph.get_text()), [])
        if len(matches) != 1 or len(matches[0].contents) < 2:
            continue
        current_anchor = matches[0].contents[1]
        if (getattr(current_anchor, 'name', None) != 'a'
                or current_anchor.get('href') != anchor['href']
                or current_anchor.get_text() != anchor.get_text()):
            continue
        first.replace_with(match[1] + match[2] + '(')
        removed += 1
    return removed


def _cnyes_remove_verified_stock_list_markers(scope, full_content, source):
    """Compare the observed stock-list separator marker, not real words.

    Public responses 4700323/4699586/4644102 retain one U+FEFF between the literal
    list separator and a company linked to its stock quote. 4663341 also has
    that prefix on the first company, immediately after the heading's BR.
    JSON-LD omits it; both current content and visible HTML retain it.
    Response 4564769 has the same separator and native quote witnesses in an
    investment-trust list, not only a foreign-investor list. Both observed
    institution headings use the same exact grammar; unknown headings remain
    unsupported. Require the same unique current paragraph, exact heading,
    marker text and both neighboring stock links. Index current paragraphs
    once, only on a genuine schema mismatch. Never change the accepted body.
    """
    from bs4 import BeautifulSoup, NavigableString, Comment

    heading = re.compile(r'^[1-9][0-9]*\. (?:外資|投信)(?:累計[買賣]超 [1-9][0-9]* 日|當日 \([0-9]{2}\) [買賣]超)的股票$')
    marker = re.compile(r'^\)、\ufeff([^\s()\ufeff]+) \($')
    first_marker = re.compile(r'^\ufeff([^\s()\ufeff]+) \($')
    current_paragraphs = None
    removed = 0

    def paragraph_key(paragraph):
        children = paragraph.contents
        if (len(children) < 3 or not isinstance(children[0], NavigableString)
                or isinstance(children[0], Comment) or not heading.fullmatch(str(children[0]))
                or getattr(children[1], 'name', None) != 'br'):
            return None
        return str(children[0]), paragraph.get_text()

    def link_key(anchor):
        if getattr(anchor, 'name', None) != 'a' or len(anchor.contents) != 1:
            return None
        text = anchor.contents[0]
        parts = urlsplit(anchor.get('href', ''))
        # The observed preceding quote may be the ETF 00715L, not a purely
        # numeric equity ID. Require an exact same current/source label;
        # neither infer a stock nor rewrite the source's symbol.
        match = re.fullmatch(r'/twstock/([0-9]{4,6}|[0-9]{5}[A-Z])', parts.path)
        if (not isinstance(text, NavigableString) or isinstance(text, Comment)
                or parts.scheme != 'https' or parts.netloc != 'www.cnyes.com'
                or not match or parts.query or parts.fragment
                or str(text) != match[1] + '-TW'):
            return None
        _cnyes_visible_after_completed_stream(anchor, None)
        return anchor['href'], str(text)

    for paragraph in scope.find_all('p'):
        key = paragraph_key(paragraph)
        if key is None:
            continue
        candidates = [node for node in paragraph.contents
                      if isinstance(node, NavigableString) and not isinstance(node, Comment)
                      and (marker.fullmatch(str(node)) or first_marker.fullmatch(str(node)))]
        for node in candidates:
            first = bool(first_marker.fullmatch(str(node)))
            before, after = link_key(node.previous_sibling), link_key(node.next_sibling)
            if (after is None or not first and before is None
                    or first and (len(paragraph.contents) < 4 or node is not paragraph.contents[2]
                                  or node.previous_sibling is not paragraph.contents[1])):
                continue
            if current_paragraphs is None:
                current = BeautifulSoup(html.unescape(full_content), source.html_parser)
                current_paragraphs = {}
                for candidate in current.find_all('p'):
                    current_key = paragraph_key(candidate)
                    if current_key is not None:
                        current_paragraphs.setdefault(current_key, []).append(candidate)
            matches = current_paragraphs.get(key, [])
            if len(matches) != 1:
                continue
            witnesses = [candidate for candidate in matches[0].contents
                         if isinstance(candidate, NavigableString) and not isinstance(candidate, Comment)
                         and str(candidate) == str(node)
                         and (candidate is matches[0].contents[2] if first
                              else link_key(candidate.previous_sibling) == before)
                         and link_key(candidate.next_sibling) == after]
            if len(witnesses) != 1:
                continue
            node.replace_with(str(node)[1:] if first else str(node).replace(')、\ufeff', ')、', 1))
            removed += 1
    return removed


_CNYES_INVESTMENT_NOTICE_LINES = (
    '本公司所推薦分析之個別有價證券',
    '無不當之財務利益關係 以往之績效不保證未來獲利',
    '投資人應獨立判斷 審慎評估並自負投資風險',
)


def _cnyes_figure_notice_text(figure):
    """Only the observed 4949007/4954707 p.warning disclaimer templates.

    A figure can contain real narrative: its class alone is not evidence to
    discard it. Require the exact three risk-disclaimer lines, one preceding
    optional textless paragraph and two line breaks, never arbitrary financial
    text. The independently retained 4954707 has no leading blank paragraph;
    neither that absence nor a real extra paragraph may be invented/removed.
    The caller proves visibility, same-src membership and current/schema
    equality and preserves this separately from both body and image caption.
    """
    from bs4 import NavigableString, Comment

    paragraphs = figure.find_all('p', recursive=False)
    if len(paragraphs) not in {1, 2}:
        return None
    if len(paragraphs) == 2 and (paragraphs[0].find(True) is not None
            or paragraphs[0].get_text(strip=True) or paragraphs[0].get('class')):
        return None
    warning = paragraphs[-1]
    if (warning.get('class') != ['warning'] or len(warning.find_all('br')) != 2
            or any(node.name != 'br' for node in warning.find_all(True))):
        return None
    for child in figure.contents:
        if isinstance(child, NavigableString):
            if isinstance(child, Comment) or str(child).strip():
                return None
        elif child.name not in {'img', 'p'}:
            return None
    text = warning.get_text('\n', strip=True)
    return text if text == '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES) else None


def _cnyes_mixed_nested_figure_identity(figure, elements, children):
    """Match the retained 4733427 mixed forest without flattening unknown DOM.

    Its source interleaves narrative paragraphs and image-only leaves, then
    an image/two-span call-to-action and a text-only link/risk-warning leaf.
    Keep ALL of that source text as one distinct supplement. The complete
    ordered DOM digest additionally proves image/paragraph placement and
    attributes, which separate text/image/link lists cannot prove. This is a
    comparison witness only: no DOM mutation, image request or schema body.
    The caller independently verifies every node's visibility and whole-schema
    equality. Legacy simple forests keep their existing fast path and ABI.
    """
    from bs4 import NavigableString, Comment

    if len(children) < 3 or any(isinstance(node, Comment) for node in figure.descendants):
        return None

    def direct_nodes(node):
        if any(isinstance(child, NavigableString) and str(child).strip() for child in node.contents):
            return None
        return node.find_all(True, recursive=False)

    def public_url(value):
        if not isinstance(value, str):
            return False
        try:
            parts = urlsplit(value)
        except ValueError:
            return False
        return parts.scheme in {'http', 'https'} and bool(parts.hostname) and not (parts.username or parts.password)

    def plain_link(link):
        return (set(link.attrs) == {'href'} and public_url(link['href'])
                and link.find(True) is None and bool(link.get_text(strip=True)))

    # Root and every nested figure remain attribute-free and depth-one.
    if any(child.attrs or child.find('figure') is not None for child in children):
        return None
    roots = list(elements)
    if roots and roots[-1].name == 'p' and not roots[-1].get_text(strip=True):
        blank = roots.pop()
        if blank.attrs or blank.find(True) is not None:
            return None
    if len(roots) < 6 or roots[0].name != 'img' or roots[-2:] != children[-2:]:
        return None
    call, footer = children[-2:]
    call_nodes, footer_nodes = direct_nodes(call), direct_nodes(footer)
    if (call_nodes is None or [node.name for node in call_nodes] != ['img', 'p']
            or footer_nodes is None or [node.name for node in footer_nodes] != ['p', 'p']):
        return None
    invitation = call_nodes[1]
    spans = direct_nodes(invitation)
    invitation_lines = ('更多盤中即時資訊', '請點擊以下連結↓↓↓')
    if (invitation.attrs or spans is None or len(spans) != 2
            or any(node.name != 'span' or node.attrs or len(node.contents) != 1
                   or not isinstance(node.contents[0], NavigableString) for node in spans)
            or tuple(node.get_text(strip=True) for node in spans) != invitation_lines):
        return None
    social, warning = footer_nodes
    social_nodes = social.find_all(True, recursive=False)
    if (social.attrs or len(social.contents) != 2 or len(social_nodes) != 1
            or not isinstance(social.contents[0], NavigableString)
            or str(social.contents[0]).strip() != '社群網站連結 :'
            or social_nodes[0].name != 'a' or not plain_link(social_nodes[0])
            or social_nodes[0].get_text(strip=True) != social_nodes[0]['href']):
        return None
    if (warning.attrs != {'class': ['warning']} or len(warning.find_all('br')) != 2
            or any(node.name != 'br' or node.attrs for node in warning.find_all(True))
            or warning.get_text('\n', strip=True) != '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES)):
        return None

    links = []
    paragraphs = 0
    image_leaf_seen = False
    paragraph_after_image_leaf = False
    for node in roots[1:-2]:
        if node.name == 'figure':
            leaf = direct_nodes(node)
            if leaf is None or len(leaf) != 1 or leaf[0].name != 'img':
                return None
            image_leaf_seen = True
        elif node.name == 'p':
            if node.attrs or not node.get_text(strip=True):
                return None
            paragraphs += 1
            paragraph_after_image_leaf |= image_leaf_seen
            for child in node.find_all(True):
                if child.parent is not node:
                    return None
                if child.name == 'strong':
                    if (child.attrs not in ({}, {'style': 'letter-spacing:0.7px; text-align:justify'})
                            or len(child.contents) != 1 or not isinstance(child.contents[0], NavigableString)
                            or not child.get_text(strip=True)):
                        return None
                elif child.name == 'a':
                    if not plain_link(child):
                        return None
                    links.append((child['href'], child.get_text(' ', strip=True)))
                elif child.name != 'br' or child.attrs:
                    return None
        else:
            return None
    if not (2 <= paragraphs <= 8 and image_leaf_seen and paragraph_after_image_leaf):
        return None
    link = social_nodes[0]
    links.append((link['href'], link.get_text(' ', strip=True)))
    image_attributes = {'src', 'srcset', 'data-height', 'data-mime', 'data-ratio', 'data-src-m', 'data-width'}
    sources = []
    for image in figure.find_all('img'):
        if (image.contents or not set(image.attrs) <= image_attributes or not public_url(image.get('src'))
                or 'data-src-m' in image.attrs and not public_url(image['data-src-m'])):
            return None
        sources.append(image['src'])
    if len(set(sources)) != len(sources):
        return None
    forest_sha256 = hashlib.sha256(str(figure).encode('utf-8')).hexdigest()
    return (figure.get_text('\n', strip=True), (tuple(sources), tuple(links), forest_sha256),
            'same_article_nested_mixed_figure_text')


def _cnyes_public_figure_url(value):
    """Validate a retained reference, not permission to request that URL."""
    if not isinstance(value, str) or any(ord(char) < 32 for char in value):
        return False
    try:
        parts = urlsplit(value)
    except ValueError:
        return False
    return parts.scheme in {'http', 'https'} and bool(parts.hostname) and not (parts.username or parts.password)


def _cnyes_empty_chart_caption_reference(figure):
    """Actual3947637: one explicitly typed chart and one NBSP caption.

    An absent textual caption neither supplies article text nor invalidates
    another real caption. This is not the arbitrary image/empty-caption
    grammar. Preserve its image reference separately and require the exact
    unique, ordered current/visible DOM witness in the caller. No image is
    requested, OCRed or interpreted as financial observations.
    """
    from bs4 import NavigableString, Comment

    elements = figure.find_all(True, recursive=False)
    if (figure.attrs or [node.name for node in elements] != ['img', 'figcaption']
            or any(isinstance(node, NavigableString) and (isinstance(node, Comment) or str(node).strip())
                   for node in figure.contents)):
        return None
    image, caption = elements
    if (set(image.attrs) != {'src', 'data-cnyeschart'} or image.contents
            or image.get('data-cnyeschart') != 'cnyeschart'
            or caption.attrs or len(caption.contents) != 1
            or not isinstance(caption.contents[0], NavigableString)
            or isinstance(caption.contents[0], Comment) or str(caption.contents[0]) != '\xa0'):
        return None
    try:
        url = urlsplit(image['src'])
    except (ValueError, TypeError):
        return None
    if (url.scheme != 'https' or url.netloc != 'cimg.cnyes.cool' or url.query or url.fragment
            or re.fullmatch(r'/prod/news/[1-9][0-9]*/o/[a-f0-9]{32}[.]jpg', url.path) is None):
        return None
    return ('', ((image['src'],), (), hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_empty_chart_caption_reference')


def _cnyes_typed_figure_images(figure):
    """Bound the new linked/gallery images; alt remains metadata, never text."""
    attributes = {'src', 'srcset', 'alt', 'data-height', 'data-mime', 'data-ratio', 'data-src-m', 'data-width'}
    images = figure.find_all('img')
    if not images or any(image.contents or not set(image.attrs) <= attributes
            or not _cnyes_public_figure_url(image.get('src'))
            or 'data-src-m' in image.attrs and not _cnyes_public_figure_url(image['data-src-m'])
            for image in images):
        return None
    sources = tuple(image['src'] for image in images)
    return sources if len(set(sources)) == len(sources) else None


def _cnyes_typed_chart_caption_reference(figure):
    """Match the observed complete typed-chart forest, not arbitrary prose.

    Actual3666458/3748731 retain one chart label p and two exact NBSP p
    nodes. 4427985/4545945/4357680/3989011/4484038/3933212 wrap one typed
    img/figcaption leaf in transparent figures, with optional NBSP captions.
    Bind the whole DOM, image and original label to the caller's ordered
    current/visible/schema proof. No image request, OCR, chart values, missing
    caption or source publication time is invented. Ordinary direct captions
    and the existing empty-chart proof retain their original contracts.
    """
    from bs4 import Comment, NavigableString
    from urllib.parse import parse_qsl

    def elements(node):
        if node.attrs or any(isinstance(child, NavigableString)
                and (isinstance(child, Comment) or str(child).strip()) for child in node.contents):
            return None
        return node.find_all(True, recursive=False)

    def plain(node):
        if (node.attrs or len(node.contents) != 1
                or not isinstance(node.contents[0], NavigableString)
                or isinstance(node.contents[0], Comment)):
            return None
        return str(node.contents[0])

    def source_url(image):
        if (set(image.attrs) != {'src', 'data-cnyeschart'} or image.contents
                or image['data-cnyeschart'] != 'cnyeschart'
                or not isinstance(image['src'], str)
                or any(ord(char) < 32 for char in image['src'])):
            return None
        try:
            parts = urlsplit(image['src'])
        except ValueError:
            return None
        if parts.scheme != 'https' or parts.fragment:
            return None
        stored_chart = (parts.netloc == 'cimg.cnyes.cool' and not parts.query
            and re.fullmatch(r'/prod/news/[1-9][0-9]*/o/[a-f0-9]{32}[.]jpg', parts.path) is not None)
        if not stored_chart:
            paths = {'/mobilechart/MobileChart.aspx': 'kchart',
                     '/mobilechart/MobileChart_TodayLive.aspx': 'linechart'}
            pairs = parse_qsl(parts.query, keep_blank_values=True)
            values = dict(pairs)
            if (parts.netloc != 'so.cnyes.com' or parts.path not in paths or len(pairs) != 6
                    or set(values) != {'markettype', 'charttype1', 'stockcode', 'imgwidth', 'imgheight', 'bgcolor'}
                    or values['markettype'] != 'twstock' or values['charttype1'] != paths[parts.path]
                    or re.fullmatch(r'[0-9]{4,6}', values['stockcode']) is None
                    or values['imgwidth'] != '580' or values['imgheight'] != '500' or values['bgcolor'] != '2'):
                return None
        return image['src']

    leaf = figure
    for depth in range(3):
        nodes = elements(leaf)
        if nodes is None:
            return None
        names = [node.name for node in nodes]
        if names in (['figure'], ['figure', 'figcaption']):
            if depth == 2 or len(nodes) == 2 and plain(nodes[1]) != '\xa0':
                return None
            leaf = nodes[0]
            continue
        break
    labels = []
    if names == ['img', 'figcaption']:
        if depth == 0:
            return None  # Do not change already supported direct-caption ABI.
        image, caption = nodes
        label = plain(caption)
        if label is None:
            return None
    elif names in (['img', 'p', 'p', 'p'], ['p', 'img', 'p', 'p']):
        image = next(node for node in nodes if node.name == 'img')
        paragraphs = [node for node in nodes if node.name == 'p']
        words = [plain(node) for node in paragraphs]
        label_index = 0 if names[0] == 'img' else 1
        if any(text != '\xa0' for index, text in enumerate(words) if index != label_index):
            return None
        label = words[label_index]
        if label is None or label == '\xa0':
            return None
    else:
        return None
    src = source_url(image)
    if src is None:
        return None
    if label != '\xa0':
        if re.fullmatch(r'[^<>\r\n]{1,48}(?:日K線圖|走勢圖)[。]?', re.sub(r'\s+', '', label)) is None:
            return None
        labels.append((label, src))
    return (figure.get_text('\n', strip=True),
            ((src,), (), hashlib.sha256(str(figure).encode('utf-8')).hexdigest(), tuple(labels)),
            'same_article_typed_chart_caption_reference')


def _cnyes_linked_caption_figure_identity(figure, elements):
    """Retained4951809: one linked caption image and one uncaptioned link leaf.

    Bind the caption only to its original outer image. The second image has
    no caption; retain its reference and both hrefs without following them.
    Full ordered DOM, current/visible visibility and whole schema remain
    independent proofs in the caller. Alt text never becomes narrative.
    """
    from bs4 import NavigableString, Comment

    if [node.name for node in elements] != ['a', 'figcaption']:
        return None
    caption = elements[1]
    nodes = caption.find_all(True, recursive=False)
    if (caption.attrs or [node.name for node in nodes] != ['p', 'figure']
            or any(isinstance(node, NavigableString) and str(node).strip() for node in caption.contents)
            or any(isinstance(node, Comment) for node in figure.descendants)):
        return None
    paragraph, leaf = nodes
    if (paragraph.attrs or len(paragraph.contents) != 1
            or not isinstance(paragraph.contents[0], NavigableString) or not paragraph.get_text(strip=True)
            or leaf.attrs or [node.name for node in leaf.find_all(True)] != ['a', 'img']
            or any(isinstance(node, NavigableString) and str(node).strip() for node in leaf.contents)):
        return None
    links = [elements[0], leaf.find('a', recursive=False)]
    for link in links:
        if (link is None or set(link.attrs) != {'href'} or not _cnyes_public_figure_url(link['href'])
                or [node.name for node in link.find_all(True)] != ['img']
                or any(isinstance(node, NavigableString) and str(node).strip() for node in link.contents)):
            return None
    if links[0]['href'] != links[1]['href']:
        return None
    images = _cnyes_typed_figure_images(figure)
    if images is None or len(images) != 2:
        return None
    text = paragraph.get_text(' ', strip=True)
    return (text, (images, tuple((link['href'], '') for link in links),
            hashlib.sha256(str(figure).encode('utf-8')).hexdigest(), ((text, images[0]),)),
            'same_article_linked_caption_figure')


def _cnyes_warning_placeholder_figure_identity(figure, elements):
    """Retained4841220: exact risk lines plus explicit NBSP placeholders.

    Never normalize arbitrary blank markup or discard extra prose as warning.
    The complete ordered forest keeps two image references and all original
    placeholders; the caller still checks membership, visibility and schema.
    """
    from bs4 import NavigableString, Comment

    if [node.name for node in elements] != ['img', 'figure', 'p', 'p', 'figure', 'p']:
        return None
    if any(isinstance(node, Comment) for node in figure.descendants):
        return None
    leaf, warning, spacer, empty, tail = elements[1:]
    if (leaf.attrs or [node.name for node in leaf.find_all(True)] != ['img']
            or any(isinstance(node, NavigableString) and str(node).strip() for node in leaf.contents)
            or warning.attrs != {'class': ['warning']}
            or [node.name for node in warning.find_all(True)] != ['br', 'br']
            or any(node.attrs or node.contents for node in warning.find_all(True))
            or warning.get_text('\n', strip=True) != '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES)):
        return None
    for placeholder in (spacer, empty, tail):
        if (placeholder.attrs or len(placeholder.contents) != 1
                or not isinstance(placeholder.contents[0], NavigableString)
                or str(placeholder.contents[0]) != '\xa0'):
            return None
    images = _cnyes_typed_figure_images(figure)
    if images is None or len(images) != 2:
        return None
    return (warning.get_text('\n', strip=True), (images, (),
            hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_warning_placeholder_figure')


def _cnyes_image_leaf_prose_figure_identity(figure, elements):
    """Retained4957610: bounded interleaved image leaves and source prose.

    This is narrative, NOT a risk-disclaimer supplement. Every source word,
    financial value, link label and final plain-text disclosure enters the
    body in its original position. Only the outer wrapper is removed in a
    private copy; nested image leaves remain excluded. The full DOM witness
    and whole current/visible/schema proof are required independently.
    """
    from bs4 import NavigableString, Comment

    names = ','.join(node.name for node in elements)
    if re.fullmatch(r'(?:figure,){1,8}(?:p,){1,64}(?:figure,){1,8}(?:p,){1,64}p', names) is None:
        return None
    leaves = [node for node in elements if node.name == 'figure']
    paragraphs = [node for node in elements if node.name == 'p']
    if (not 2 <= len(leaves) <= 8 or not 2 <= len(paragraphs) <= 64
            or any(isinstance(node, Comment) for node in figure.descendants)):
        return None
    for leaf in leaves:
        if (leaf.attrs or [node.name for node in leaf.find_all(True)] != ['img']
                or any(isinstance(node, NavigableString) and str(node).strip() for node in leaf.contents)):
            return None
    warning = paragraphs[-1]
    if (warning.attrs != {'class': ['warning']} or len(warning.contents) != 1
            or not isinstance(warning.contents[0], NavigableString) or not warning.get_text(strip=True)):
        return None
    links = []
    for paragraph in paragraphs[:-1]:
        if paragraph.attrs or not paragraph.get_text(strip=True):
            return None
        for node in paragraph.find_all(True):
            if (node.name != 'a' or node.parent is not paragraph or set(node.attrs) != {'href'}
                    or not _cnyes_public_figure_url(node['href']) or len(node.contents) != 1
                    or not isinstance(node.contents[0], NavigableString) or not node.get_text(strip=True)):
                return None
            links.append((node['href'], node.get_text(' ', strip=True)))
    images = _cnyes_typed_figure_images(figure)
    if images is None or len(images) != len(leaves):
        return None
    return (figure.get_text('\n', strip=True), (images, tuple(links),
            hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_image_leaf_prose_figure')


def _cnyes_linked_figure_identity(figure, elements):
    """The retained 4902745 image/link paragraph and textless linked image.

    Keep the complete ordered DOM in the comparison witness. An anchor's
    href cannot disappear through the legacy textless-image fast path. The
    caller verifies current/visible/schema agreement; no link is followed.
    """
    from bs4 import NavigableString, Comment

    if any(isinstance(node, Comment) for node in figure.descendants):
        return None
    sources = _cnyes_typed_figure_images(figure)
    if sources is None or len(sources) != 1:
        return None
    names = [node.name for node in elements]
    if names == ['img', 'p']:
        paragraph = elements[1]
        if (paragraph.attrs or len(paragraph.contents) != 2
                or not isinstance(paragraph.contents[0], NavigableString)
                or not str(paragraph.contents[0]).strip()
                or getattr(paragraph.contents[1], 'name', None) != 'a'):
            return None
        link = paragraph.contents[1]
        if (len(link.contents) != 1 or not isinstance(link.contents[0], NavigableString)
                or link.get_text(strip=True) != link.get('href')):
            return None
        kind = 'same_article_image_figure_link_text'
    elif names == ['a']:
        link = elements[0]
        if (link.find_all(True, recursive=False) != figure.find_all('img')
                or any(isinstance(node, NavigableString) and str(node).strip() for node in link.contents)):
            return None
        kind = 'same_article_linked_image_reference'
    else:
        return None
    if set(link.attrs) != {'href'} or not _cnyes_public_figure_url(link['href']):
        return None
    return (figure.get_text('\n', strip=True),
            (sources, ((link['href'], link.get_text(' ', strip=True)),),
             hashlib.sha256(str(figure).encode('utf-8')).hexdigest()), kind)


def _cnyes_caption_gallery_identity(figure, elements):
    """Bind each original caption to its image in the retained 4795333 gallery.

    Its rich outer figcaption contains one caption paragraph, image/caption
    leaves and a bounded group of leaves. Preserve repeated captions, original
    numerical text and source order. Unknown markup is not flattened. The
    complete DOM witness and full-schema comparison remain independent of
    this grammar; neither alt nor pixels become article text.
    """
    from bs4 import NavigableString, Comment

    if [node.name for node in elements] != ['img', 'figcaption']:
        return None
    if any(isinstance(node, Comment) for node in figure.descendants):
        return None
    sources = _cnyes_typed_figure_images(figure)
    if sources is None or len(sources) < 2:
        return None

    def children(node):
        if node.attrs or any(isinstance(child, NavigableString) and str(child).strip() for child in node.contents):
            return None
        return node.find_all(True, recursive=False)

    def plain_text(node):
        return (not node.attrs and len(node.contents) == 1
                and isinstance(node.contents[0], NavigableString) and bool(node.get_text(strip=True)))

    def blank_paragraph(node):
        return node.name == 'p' and not node.attrs and len(node.contents) == 1 and str(node.contents[0]) == '\xa0'

    pairs = []

    def leaf(node):
        items = children(node)
        if (items is None or [item.name for item in items] != ['img', 'figcaption']
                or not plain_text(items[1])):
            return False
        pairs.append((items[1].get_text(' ', strip=True), items[0]['src']))
        return True

    caption_nodes = children(elements[1])
    if (caption_nodes is None or len(caption_nodes) < 3 or caption_nodes[0].name != 'p'
            or not plain_text(caption_nodes[0])):
        return None
    # Actual4631268 contains p/caption, one image/caption leaf, then a real
    # narrative p inside its outer caption. A NBSP gallery spacer is not the
    # only valid ending. Keep this distinct typed shape and the entire DOM;
    # the caller still proves current/visible/order and the complete schema.
    narrative_tail = ([node.name for node in caption_nodes] == ['p', 'figure', 'p']
                      and len(sources) == 2 and plain_text(caption_nodes[-1]))
    if not narrative_tail and not blank_paragraph(caption_nodes[-1]):
        return None
    pairs.append((caption_nodes[0].get_text(' ', strip=True), elements[0]['src']))
    for node in caption_nodes[1:-1]:
        if node.name != 'figure':
            return None
        if leaf(node):
            continue
        group = children(node)
        if (group is None or not 3 <= len(group) <= 9 or not blank_paragraph(group[-1])
                or any(item.name != 'figure' or not leaf(item) for item in group[:-1])):
            return None
    if tuple(src for _, src in pairs) != sources:
        return None
    witness = (sources, tuple(pairs), hashlib.sha256(str(figure).encode('utf-8')).hexdigest())
    if narrative_tail:
        return (figure.get_text('\n', strip=True),
                (*witness, caption_nodes[-1].get_text('\n', strip=True)),
                'same_article_caption_gallery_narrative_tail')
    return figure.get_text('\n', strip=True), witness, 'same_article_caption_gallery'


def _cnyes_one_line_warning_supplement_identity(figure, elements):
    """Retained 5125945: two source links and one exact risk-disclaimer line.

    This is the entire visible/current figure text, not arbitrary prose
    discarded as a warning. The caller proves whole schema agreement, node
    visibility and source order. Keep all links/text separately and hash the
    complete DOM; no linked page or image is requested.
    """
    from bs4 import NavigableString, Comment

    if [node.name for node in elements] != ['img', 'p', 'p', 'p']:
        return None
    if any(isinstance(node, Comment) for node in figure.descendants):
        return None
    images = _cnyes_typed_figure_images(figure)
    if images is None or len(images) != 1:
        return None
    warning = elements[-1]
    exact_warning = '本公司所分析個別有價證券僅供參考，投資人應獨立判斷，審慎評估並自負投資風險。'
    if (warning.attrs != {'class': ['warning']} or len(warning.contents) != 1
            or not isinstance(warning.contents[0], NavigableString) or str(warning.contents[0]) != exact_warning):
        return None
    links = []
    for paragraph, label in zip(elements[1:-1], ('▶進一步了解資產管理處', '▶閱讀更多精彩文章'), strict=True):
        meaningful = [node for node in paragraph.contents
                      if not isinstance(node, NavigableString) or str(node).strip()]
        if (paragraph.attrs or len(meaningful) != 2 or not isinstance(meaningful[0], NavigableString)
                or str(meaningful[0]).strip() != label or getattr(meaningful[1], 'name', None) != 'a'):
            return None
        link = meaningful[1]
        if (set(link.attrs) != {'href'} or not _cnyes_public_figure_url(link['href'])
                or len(link.contents) != 1 or not isinstance(link.contents[0], NavigableString)
                or link.get_text(strip=True) != link['href']):
            return None
        links.append((link['href'], link.get_text(' ', strip=True)))
    return (figure.get_text('\n', strip=True),
            (images, tuple(links), hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_image_figure_one_line_warning_supplement')


def _cnyes_social_figure_identity(figure, elements):
    """Actual4778922's three explicit social links, not arbitrary figure prose.

    The free current content and visible DOM retain the same image, labels,
    links and ordered forest; schema includes the whole supplement. Preserve
    these as typed source notices without requesting any linked service.
    Unknown labels, attributes, nesting and extra words still fail closed.
    """
    from bs4 import NavigableString, Comment

    if ([node.name for node in elements] != ['img', 'p', 'p', 'p', 'p']
            or any(isinstance(node, Comment) for node in figure.descendants)
            or any(node.attrs for node in elements[1:])):
        return None
    image = elements[0]
    image_attributes = {'src', 'srcset', 'data-height', 'data-mime', 'data-ratio', 'data-src-m', 'data-width'}
    if (image.contents or not set(image.attrs) <= image_attributes
            or not _cnyes_public_figure_url(image.get('src'))
            or 'data-src-m' in image.attrs and not _cnyes_public_figure_url(image['data-src-m'])):
        return None
    label = elements[3]
    if (len(label.contents) != 1 or not isinstance(label.contents[0], NavigableString)
            or str(label.contents[0]) != '新龍捲風操盤戰隊 Line@生活圈'):
        return None
    links = []
    for paragraph, prefix, host, pattern in (
        (elements[1], 'TG：', 't.me', r'/[A-Za-z0-9_]{1,64}'),
        (elements[2], 'FB 社團：', 'www.facebook.com', r'/groups/[A-Za-z0-9_.-]{1,128}'),
        (elements[4], '', 'line.me', r'/R/ti/p/%40[A-Za-z0-9_-]{1,64}'),
    ):
        nodes = paragraph.contents
        if prefix:
            if (len(nodes) != 2 or not isinstance(nodes[0], NavigableString) or str(nodes[0]) != prefix):
                return None
            link = nodes[1]
        else:
            if len(nodes) != 1:
                return None
            link = nodes[0]
        if (getattr(link, 'name', None) != 'a' or set(link.attrs) != {'href'}
                or len(link.contents) != 1 or not isinstance(link.contents[0], NavigableString)
                or not _cnyes_public_figure_url(link.get('href')) or str(link.contents[0]) != link['href']):
            return None
        parts = urlsplit(link['href'])
        if (parts.scheme != 'https' or parts.netloc != host or parts.query or parts.fragment
                or re.fullmatch(pattern, parts.path) is None):
            return None
        links.append((link['href'], str(link.contents[0])))
    return (figure.get_text('\n', strip=True), ((image['src'],), tuple(links),
            hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_image_figure_social_links')


def _cnyes_social_placeholder_figure_identity(figure, elements):
    """Retained4825496: one NBSP followed by four labelled social references.

    The placeholder is not missing narrative. Preserve every original label,
    link text and href, including the source's fbclid, as a separate notice.
    Do not follow short links or treat the image/alt as financial observations.
    Full current/visible DOM order and whole schema remain caller proofs.
    """
    from bs4 import NavigableString, Comment

    if ([node.name for node in elements] != ['img', 'p', 'p', 'p', 'p', 'p']
            or any(isinstance(node, Comment) for node in figure.descendants)
            or any(node.attrs for node in elements[1:])):
        return None
    placeholder = elements[1]
    if (len(placeholder.contents) != 1 or not isinstance(placeholder.contents[0], NavigableString)
            or str(placeholder.contents[0]) != '\xa0'):
        return None
    images = _cnyes_typed_figure_images(figure)
    if images is None or len(images) != 1:
        return None
    links = []
    specifications = (
        ('梅姊 LINE ID :', 'lin.ee', r'/[A-Za-z0-9]{1,64}', False),
        ('TG 盤中即時資訊 :', 't.me', r'/[A-Za-z0-9_]{1,64}', True),
        ('梅姊的 Instagram ：', 'reurl.cc', r'/[A-Za-z0-9]{1,64}', False),
        ('YouTube 頻道 :', 'lihi1.com', r'/[A-Za-z0-9]{1,64}', True),
    )
    for paragraph, (prefix, host, pattern, tracking_allowed) in zip(elements[2:], specifications, strict=True):
        if (len(paragraph.contents) != 2 or not isinstance(paragraph.contents[0], NavigableString)
                or str(paragraph.contents[0]) not in {prefix + ' ', prefix + '\xa0'}):
            return None
        link = paragraph.contents[1]
        if (getattr(link, 'name', None) != 'a' or not set(link.attrs) <= {'href', 'target'}
                or 'target' in link.attrs and link['target'] != '_blank'
                or len(link.contents) != 1 or not isinstance(link.contents[0], NavigableString)
                or not _cnyes_public_figure_url(link.get('href'))):
            return None
        parts = urlsplit(link['href'])
        if (parts.scheme != 'https' or parts.netloc != host or parts.fragment
                or re.fullmatch(pattern, parts.path) is None
                or parts.query and (not tracking_allowed or re.fullmatch(r'fbclid=[A-Za-z0-9_-]{1,512}', parts.query) is None)
                or str(link.contents[0]) != f'https://{host}{parts.path}'):
            return None
        links.append((link['href'], str(link.contents[0])))
    return (figure.get_text('\n', strip=True), (images, tuple(links),
            hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_image_figure_social_placeholder_links')


def _cnyes_caption_leaf_placeholder_identity(figure, elements):
    """Retained5164089: one ordinary caption leaf plus one exact NBSP p.

    A wrapper cannot change caption ownership: associate the original text
    with its inner image, never a neighbouring image with the same caption.
    Preserve full ordered DOM identity; unknown text/attrs/nesting fail closed.
    """
    from bs4 import NavigableString, Comment

    if ([node.name for node in elements] != ['figure', 'p']
            or any(isinstance(node, Comment) for node in figure.descendants)):
        return None
    leaf, placeholder = elements
    if (leaf.attrs or [node.name for node in leaf.find_all(True, recursive=False)] != ['img', 'figcaption']
            or any(isinstance(node, NavigableString) and str(node).strip() for node in leaf.contents)
            or placeholder.attrs or len(placeholder.contents) != 1
            or not isinstance(placeholder.contents[0], NavigableString)
            or str(placeholder.contents[0]) != '\xa0'):
        return None
    caption = leaf.find('figcaption', recursive=False)
    if (caption.attrs or len(caption.contents) != 1 or not isinstance(caption.contents[0], NavigableString)
            or not caption.get_text(strip=True)):
        return None
    images = _cnyes_typed_figure_images(figure)
    if images is None or len(images) != 1:
        return None
    text = caption.get_text(' ', strip=True)
    return (text, (images, (), hashlib.sha256(str(figure).encode('utf-8')).hexdigest(), ((text, images[0]),)),
            'same_article_caption_leaf_placeholder')


def _cnyes_figure_source_byline_identity(figure, elements):
    """Retained4937652's text-only source credit, never an inferred author.

    The caller still matches the unique ordered whole current/visible forest,
    its visibility and the entire schema. This does not permit arbitrary prose,
    hidden content, links or images to become a credit-only supplement.
    """
    from bs4 import NavigableString, Comment

    if [node.name for node in elements] != ['strong']:
        return None
    strong = elements[0]
    if (set(strong.attrs) != {'style'}
            or re.sub(r'\s+', '', strong['style']).rstrip(';') != 'letter-spacing:0.7px;text-align:justify'
            or len(strong.contents) != 1 or not isinstance(strong.contents[0], NavigableString)
            or isinstance(strong.contents[0], Comment)):
        return None
    text = str(strong.contents[0])
    if re.fullmatch(r'文．[^\s<>]{1,64}', text) is None:
        return None
    return (text, ((), (), hashlib.sha256(str(figure).encode('utf-8')).hexdigest()),
            'same_article_figure_source_byline')


def _cnyes_image_paragraph_figure_identity(figure):
    """Corroborate bounded native image/paragraph blocks, not generic figures.

    Actual4915612/4949847 retain plain span paragraphs;4726655 interleaves
    one image leaf;4726668/4840208/4788946 retain image-leaf paragraph forests;
    5034210 has bold paragraphs and bold public-link labels. Preserve every
    narrative paragraph in its original position, including source disclosures.
    Explicit4791485圖文 labels and4498141 visible span/alt labels are captions,
    never invented prose or an interpretation of pixels. Exact ordered DOM,
    visibility and whole current/visible/schema agreement remain independent
    requirements in the caller. Existing classifiers run first and keep their
    ABI. No media/link request, OCR, source clock or author is inferred here.
    """
    from bs4 import Comment, NavigableString

    if (figure.attrs or any(isinstance(node, Comment) for node in figure.descendants)
            or any(isinstance(node, NavigableString) and str(node).strip() for node in figure.contents)):
        return None
    elements = figure.find_all(True, recursive=False)
    names = ','.join(node.name for node in elements)
    images = _cnyes_typed_figure_images(figure)
    if images is None:
        return None
    digest = hashlib.sha256(str(figure).encode()).hexdigest()
    if names in {'img,p', 'img,span'}:
        label = elements[1]
        text = label.get_text(' ', strip=True)
        explicit = (names == 'img,p' and text.startswith('圖文：')
            or names == 'img,span' and text and isinstance(elements[0].get('alt'), str)
                and re.sub(r'\s+', '', text) == re.sub(r'\s+', '', elements[0]['alt']))
        if (explicit and len(images) == 1 and not label.attrs and len(label.contents) == 1
                and isinstance(label.contents[0], NavigableString) and text):
            return (text, (images, (), digest, ((text, images[0]),)),
                'same_article_labelled_image_paragraph_caption')
    if (re.fullmatch(r'(?:img|figure),(?:p,){0,31}p', names) is None
            and names != 'img,p,figure,p,p'):
        return None
    leaves = [node for node in elements if node.name == 'figure']
    for leaf in leaves:
        if (leaf.attrs or [node.name for node in leaf.find_all(True)] != ['img']
                or any(isinstance(node, NavigableString) and str(node).strip() for node in leaf.contents)):
            return None
    paragraphs = [node for node in elements if node.name == 'p']
    if not paragraphs or len(images) != len(leaves) + sum(node.name == 'img' for node in elements):
        return None
    # The direct-image family is the retained span/bold paragraph layout.
    # Plain social-link blocks keep their own independently reviewed grammar;
    # a rejected social prefix/URL must not fall through as general prose.
    if not leaves and not all(paragraph.find(['span', 'strong']) for paragraph in paragraphs):
        return None
    links = []
    for paragraph in paragraphs:
        # An untyped chart label must not evade the independently typed chart
        # contract through a more general paragraph-preservation path.
        if re.fullmatch(r'[^<>\r\n]{1,48}(?:日K線圖|走勢圖)[。]?',
                re.sub(r'\s+', '', paragraph.get_text(' ', strip=True))):
            return None
        if paragraph.attrs:
            if (paragraph is not elements[-1] or paragraph.attrs != {'class': ['warning']}
                    or paragraph.get_text('\n', strip=True) != '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES)
                    or [node.name for node in paragraph.find_all(True)] != ['br', 'br']
                    or any(node.attrs or node.contents for node in paragraph.find_all(True))):
                return None
            continue
        if not paragraph.get_text(strip=True):
            if len(paragraph.contents) != 1 or str(paragraph.contents[0]) != '\xa0':
                return None
            continue
        for node in paragraph.find_all(True, recursive=False):
            if node.name in {'span', 'strong'}:
                if (node.attrs not in ({}, {'style': 'letter-spacing:0.7px; text-align:justify'})
                        or len(node.contents) != 1 or not isinstance(node.contents[0], NavigableString)
                        or not node.get_text(strip=True) and str(node.contents[0]) != '\xa0'):
                    return None
            elif node.name == 'a':
                attrs = {'href': node.get('href'), 'target': '_self', 'rel': ['noopener', 'noreferrer'],
                    'data-ga-target': 'news', 'data-ga-event-name': 'Click_Quote',
                    'data-ga-section': 'News_Article_文中行情', 'data-ga-click-item': node.get('data-ga-click-item')}
                quote = (node.attrs == attrs and isinstance(node.get('data-ga-click-item'), str)
                    and re.fullmatch(r'TWS:[0-9]{4,6}:STOCK:COMMON', node['data-ga-click-item']) is not None
                    and node['href'] == 'https://www.cnyes.com/twstock/' + node['data-ga-click-item'].split(':')[1])
                nested = node.find_all(True, recursive=False)
                strong_link = (len(node.contents) == 1 and len(nested) == 1 and nested[0].name == 'strong'
                    and not nested[0].attrs and len(nested[0].contents) == 1
                    and isinstance(nested[0].contents[0], NavigableString))
                plain = len(node.contents) == 1 and isinstance(node.contents[0], NavigableString)
                if (not (set(node.attrs) == {'href'} or quote
                            or strong_link and set(node.attrs) == {'href', 'target'} and node['target'] == '_blank')
                        or not _cnyes_public_figure_url(node.get('href')) or not (plain or strong_link)
                        or not node.get_text(strip=True)):
                    return None
                links.append((node['href'], node.get_text(' ', strip=True)))
            elif node.name != 'br' or node.attrs or node.contents:
                return None
    return (figure.get_text('\n', strip=True), (images, tuple(links), digest),
        'same_article_image_paragraph_narrative')



_CNYES_ORDERED_FIGURE_KINDS = frozenset({
    'same_article_ordered_image_leaf_paragraphs', 'same_article_unpaired_figcaption_text',
    'same_article_figure_source_invitation', 'same_article_image_leaf_numbered_heading',
    'same_article_image_licensed_notice', 'same_article_image_leaf_comparison_label',
    'same_article_image_leaf_placeholder_forest', 'same_article_image_forest_source_credit',
    'same_article_matched_figure_paragraphs',
})


def _cnyes_ordered_figure_forest_identity(figure):
    """Describe bounded source image forests, with explicit text roles.

    Current/visible whole DOM, every node visibility and complete schema are
    proved independently by the caller. Only image leaves are recursive;
    unknown nested prose, links, captions, charts and warnings remain gaps.
    Preserve unpaired captions without inventing image pairs, credits without
    reassigning authors and labels without OCR or fetching their references.
    Existing reviewed classifiers run first and retain their output ABI.
    """
    from bs4 import Comment, NavigableString

    nodes = figure.find_all(True)
    if (figure.attrs or len(nodes) > 256 or len(figure.find_all('figure')) > 16
            or any(isinstance(node, Comment) for node in figure.descendants)):
        return None
    digest = hashlib.sha256(str(figure).encode()).hexdigest()
    images = _cnyes_typed_figure_images(figure) if figure.find('img') else ()
    if images is None:
        return None
    links = []

    def direct(node):
        if any(isinstance(child, NavigableString) and str(child).strip() for child in node.contents):
            return None
        return node.find_all(True, recursive=False)

    def leaf(node):
        return (node.name == 'figure' and not node.attrs and [child.name for child in node.find_all(True)] == ['img']
            and direct(node) is not None)

    def inline(node):
        for child in node.find_all(True):
            if child.name in {'span', 'strong', 'em'}:
                style = re.sub(r'\s+', '', child.get('style', '')).rstrip(';')
                if child.attrs and not (set(child.attrs) == {'style'} and
                        style == 'letter-spacing:0.7px;text-align:justify'):
                    return False
            elif child.name == 'br':
                if child.attrs or child.contents:
                    return False
            elif child.name == 'a':
                symbol = child.get('data-ga-click-item')
                quote_attrs = {'href': child.get('href'), 'target': '_self', 'rel': ['noopener', 'noreferrer'],
                    'data-ga-target': 'news', 'data-ga-event-name': 'Click_Quote',
                    'data-ga-section': 'News_Article_文中行情', 'data-ga-click-item': symbol}
                quote = (isinstance(symbol, str) and re.fullmatch(r'TWS:[0-9]{4,6}:STOCK:COMMON', symbol)
                    and child.attrs == quote_attrs and child['href'] == 'https://www.cnyes.com/twstock/' + symbol.split(':')[1])
                if (not (set(child.attrs) == {'href'} or quote) or not _cnyes_public_figure_url(child.get('href'))
                        or not child.get_text(strip=True) or any(n.name != 'strong' or n.attrs for n in child.find_all(True))):
                    return False
                links.append((child['href'], child.get_text(' ', strip=True)))
            else:
                return False
        return True

    def warning(node):
        if node.attrs != {'class': ['warning']}:
            return False
        text = node.get_text('\n', strip=True)
        exact = text == '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES)
        licensed = re.fullmatch(
            r'\([0-9]{3}\)金管投顧字第[0-9]{3}號，本公司不保證獲利，投資人應獨立判斷，審慎評估並自負投資風險。'
            r'[^<>\r\n]{1,100}證券投資顧問股份有限公司[^<>\r\n]{1,80}分析師客服專線：[0-9-]{8,20}',
            re.sub(r'\s+', '', text)) is not None
        return (exact or licensed) and all(n.name == 'br' and not n.attrs and not n.contents for n in node.find_all(True))

    def result(kind, text=None):
        return (figure.get_text('\n', strip=True) if text is None else text,
            (images, tuple(links), digest), kind)

    elements = direct(figure)
    if elements is None:
        return None
    names = [node.name for node in elements]
    if names == ['figcaption'] and not images:
        caption = elements[0]
        descendants = caption.find_all(True)
        if (not caption.attrs and len(descendants) == 1 and descendants[0].name == 'span'
                and len(caption.contents) == 1 and len(descendants[0].contents) == 1
                and isinstance(descendants[0].contents[0], NavigableString)
                and caption.get_text(strip=True) and inline(caption)):
            return result('same_article_unpaired_figcaption_text')
        return None
    if names == ['span'] and not images:
        span = elements[0]
        if not span.attrs and len(span.contents) == 1 and str(span.contents[0]) == '《獲得更多「獨家」盤中資訊》':
            return result('same_article_figure_source_invitation')
        return None
    if names == ['h6', 'figure'] and len(images) == 1 and leaf(elements[1]):
        heading = elements[0]
        if (not heading.attrs and len(heading.contents) == 1 and getattr(heading.contents[0], 'name', None) == 'em'
                and not heading.contents[0].attrs and inline(heading)
                and re.fullmatch(r'[一二三四五六七八九十]{1,3}、[^<>\r\n]{1,120}', heading.get_text(' ', strip=True))):
            return result('same_article_image_leaf_numbered_heading')
        return None
    if names == ['img', 'p'] and len(images) == 1 and warning(elements[1]):
        image, notice = elements
        if isinstance(image.get('alt'), str) and re.sub(r'\s+', '', image['alt']) == re.sub(r'\s+', '', notice.get_text()):
            return result('same_article_image_licensed_notice')
        return None
    if (names == ['p', 'figure', 'figure', 'p'] and all(leaf(node) for node in elements[1:3])
            and len(images) == 2):
        label, spacer = elements[0], elements[-1]
        spans = label.find_all(True)
        if (not label.attrs and len(label.contents) == len(spans) == 1 and spans[0].name == 'span'
                and spans[0].attrs == {'style': 'background-color:#FFFF00'} and spans[0].get_text() == '比較表'
                and len(spans[0].contents) == 1 and not spacer.attrs and len(spacer.contents) == 1
                and str(spacer.contents[0]) == '\xa0'):
            return result('same_article_image_leaf_comparison_label')
    if images and not figure.get_text(strip=True):
        def empty_forest(node, depth=0):
            children = direct(node)
            if depth > 2 or node.attrs or children is None:
                return False
            for child in children:
                if child.name == 'img':
                    continue
                if child.name == 'p' and not child.attrs and len(child.contents) == 1 and str(child.contents[0]) == '\xa0':
                    continue
                if child.name != 'figure' or not empty_forest(child, depth + 1):
                    return False
            return bool(children)
        if empty_forest(figure):
            return result('same_article_image_leaf_placeholder_forest', '')
        return None
    if images and re.fullmatch(r'作者：[^<>\r\n]{1,100}分析師', figure.get_text(' ', strip=True)):
        def credit_forest(node, depth=0):
            children = direct(node)
            if depth > 2 or node.attrs or children is None:
                return False
            for child in children:
                if child.name == 'img':
                    continue
                if child.name == 'figure' and credit_forest(child, depth + 1):
                    continue
                if child.name == 'p' and not child.attrs and inline(child):
                    continue
                return False
            return bool(children)
        if credit_forest(figure):
            return result('same_article_image_forest_source_credit')
        return None
    # Only depth-one, attribute-free image leaves plus typed paragraphs. A
    # nested prose/caption/social figure, direct-image-only social block,
    # unknown chart label or arbitrary root text cannot fall through here.
    leaves = [node for node in elements if node.name == 'figure']
    if not images or not leaves or not all(leaf(node) for node in leaves):
        return None
    paragraphs = [node for node in elements if node.name == 'p']
    footers = [node for node in elements if node.name == 'div']
    if len(footers) > 1 or footers and footers[0] is not elements[-1]:
        return None
    if footers:
        footer = footers[0]
        children = direct(footer)
        if footer.attrs or children is None or len(children) != 1 or children[0].name != 'p' or not warning(children[0]):
            return None
    if not 1 <= len(paragraphs) <= 32 or any(node.name not in {'img', 'figure', 'p', 'div'} for node in elements):
        return None
    if len(images) != len(leaves) + sum(node.name == 'img' for node in elements):
        return None
    for paragraph in paragraphs:
        text = paragraph.get_text(' ', strip=True)
        if paragraph.attrs:
            if paragraph is not elements[-1] or not warning(paragraph):
                return None
        elif not text:
            if len(paragraph.contents) != 1 or str(paragraph.contents[0]) != '\xa0':
                return None
        elif (re.fullmatch(r'[^<>\r\n]{1,48}(?:日K線圖|走勢圖)[。]?', re.sub(r'\s+', '', text))
                or not inline(paragraph)):
            return None
    return result('same_article_ordered_image_leaf_paragraphs')



def _cnyes_matched_figure_paragraph_identity(figure):
    """Describe public paragraph-bearing figures, not arbitrary figure text.

    The caller independently matches each whole current/visible DOM in source
    order, every node's visibility and the complete schema. Keep source p text
    as paragraphs, explicit figcaptions as image metadata and link/image/DOM
    references separately. No image requests, OCR, inferred captions or authors.
    Existing typed grammars run first, so their record ABI remains unchanged.
    """
    from bs4 import Comment, NavigableString

    paragraphs = figure.find_all('p')
    if not paragraphs or not 1 <= len(paragraphs) <= 32 or figure.attrs:
        return None
    nodes = figure.find_all(True)
    if (len(nodes) > 256 or len(figure.find_all('figure')) > 16
            or any(isinstance(n, Comment) for n in figure.descendants)):
        return None
    images = _cnyes_typed_figure_images(figure) if figure.find('img') else ()
    if images is None or len(images) > 16:
        return None
    links = []
    for block in (figure, *figure.find_all('figure')):
        if (block.attrs or len(block.find_parents('figure')) > 2
                or any(isinstance(n, NavigableString) and str(n).strip() for n in block.contents)
                or any(n.name not in {'img', 'p', 'figure', 'figcaption'}
                       for n in block.find_all(True, recursive=False))):
            return None
        if block is not figure and [n.name for n in block.find_all(True)] != ['img']:
            return None  # Only an image leaf may nest; no new nested prose grammar.
    for paragraph in paragraphs:
        if (paragraph.attrs not in ({}, {'class': ['warning']})
                or paragraph.find_parent('figcaption') is not None
                or paragraph.find(['img', 'figure', 'figcaption', 'p']) is not None):
            return None
        if (paragraph.attrs == {'class': ['warning']}
                and paragraph.get_text('\n', strip=True) != '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES)):
            return None  # Existing licensed/source-credit notice grammars keep their own proof.
        if re.fullmatch(r'[^<>\r\n]{1,48}(?:日K線圖|走勢圖)[。]?',
                        re.sub(r'\s+', '', paragraph.get_text(' ', strip=True))):
            return None  # A chart label is not newly inferred narrative.
        if paragraph.get_text(' ', strip=True).startswith((
                'TG：', 'FB 社團：', '新龍捲風操盤戰隊 Line@生活圈',
                '梅姊 LINE ID :', 'TG 盤中即時資訊 :', '梅姊的 Instagram ：', 'YouTube 頻道 :')):
            return None  # Social supplements keep their typed labels/URL/placeholder proof.
    captions = figure.find_all('figcaption')
    if captions and (len(captions) != 1 or len(images) != 1 or captions[0].parent is not figure
                     or captions[0].attrs or not captions[0].get_text(strip=True)
                     or figure.find('img').parent is not figure):
        return None  # Never guess an image/caption pairing in a mixed forest.
    for node in nodes:
        if node.name in {'figure', 'p', 'img', 'figcaption'}:
            continue
        if node.name in {'strong', 'span', 'em', 'u'}:
            if node.attrs:
                return None
        elif node.name == 'br':
            if node.attrs or node.contents:
                return None
        elif node.name == 'a':
            if not _cnyes_public_figure_url(node.get('href')) or not node.get_text(strip=True):
                return None
            symbol = node.get('data-ga-click-item')
            quote = (isinstance(symbol, str) and re.fullmatch(r'TW[SG]:[0-9]{4,6}:STOCK:COMMON', symbol)
                and node.attrs == {'href': 'https://www.cnyes.com/twstock/' + symbol.split(':')[1],
                    'target': '_self', 'rel': ['noopener', 'noreferrer'], 'data-ga-target': 'news',
                    'data-ga-event-name': 'Click_Quote', 'data-ga-section': 'News_Article_文中行情',
                    'data-ga-click-item': symbol})
            literal_link = (set(node.attrs) <= {'href', 'target', 'rel'}
                and node.get('target', '_self') in {'_self', '_blank'}
                and isinstance(node.get('rel', []), list)
                and set(node.get('rel', [])) <= {'noopener', 'noreferrer'})
            if not (quote or literal_link):
                return None
            links.append((node['href'], node.get_text(' ', strip=True)))
        else:
            return None
        if not node.find_parent(['p', 'figcaption']):
            return None
    if not any(p.get_text(strip=True) for p in paragraphs):
        return None
    pairs = tuple((caption.get_text(' ', strip=True), images[0]) for caption in captions)
    return (figure.get_text('\n', strip=True),
        (images, tuple(links), hashlib.sha256(str(figure).encode()).hexdigest(), pairs),
        'same_article_matched_figure_paragraphs')


def _cnyes_structured_figure_identity(figure):
    """Describe the observed bounded source figure/supplement structures.

    The native renderer preserves an image-only child figure inside a caption
    or a publisher supplement ending in the exact risk warning. Match the
    complete ordered text, images and links in current/visible content; do not
    flatten arbitrary nesting or reclassify unknown prose as a risk warning.
    4822498 has the same supplement directly below one image, without child
    figures. It still needs ordered plain paragraphs, exact links, the final
    known warning and whole current/visible/schema agreement.
    The caller verifies visibility and the entire schema comparison separately.
    """
    from bs4 import NavigableString, Comment

    if figure.attrs:
        return None
    elements = figure.find_all(True, recursive=False)
    if any(isinstance(node, NavigableString) and (isinstance(node, Comment) or str(node).strip())
           for node in figure.contents):
        return None
    byline = _cnyes_figure_source_byline_identity(figure, elements)
    if byline is not None:
        return byline
    gallery = _cnyes_caption_gallery_identity(figure, elements)
    if gallery is not None:
        return gallery
    for identify in (_cnyes_linked_caption_figure_identity, _cnyes_warning_placeholder_figure_identity,
                     _cnyes_image_leaf_prose_figure_identity, _cnyes_social_placeholder_figure_identity,
                     _cnyes_caption_leaf_placeholder_identity):
        key = identify(figure, elements)
        if key is not None:
            return key
    linked = _cnyes_linked_figure_identity(figure, elements)
    if linked is not None:
        return linked
    single_warning = _cnyes_one_line_warning_supplement_identity(figure, elements)
    if single_warning is not None:
        return single_warning
    social_links = _cnyes_social_figure_identity(figure, elements)
    if social_links is not None:
        return social_links
    children = [node for node in elements if node.name == 'figure']
    if len(children) > 8:
        return None
    for child in children:
        descendants = child.find_all(True)
        if (child.attrs or len(descendants) != 1
                or descendants[0].name != 'img' or descendants[0].parent is not child
                or any(isinstance(node, NavigableString)
                       and (isinstance(node, Comment) or str(node).strip()) for node in child.contents)):
            # Only the independently retained mixed grammar may replace this
            # rejection. A random nested paragraph or unknown warning stays
            # unsupported even when the schema repeats the same words.
            return _cnyes_mixed_nested_figure_identity(figure, elements, children)
    images = figure.find_all('img')
    sources = []
    for image in images:
        src = image.get('src')
        if not isinstance(src, str):
            return None
        try:
            parts = urlsplit(src)
        except ValueError:
            return None
        if parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username or parts.password:
            return None
        sources.append(src)
    if not sources or len(set(sources)) != len(sources):
        return None
    if [node.name for node in elements] == ['img', 'p']:
        # Actual 5309063: an image plus p/em/strong source credit. This is
        # neither a caption nor the article's author. Preserve its full text
        # separately; the caller still proves same-image/current/visible/order
        # and whole-schema equality. Do not flatten arbitrary nested prose.
        paragraph = elements[1]
        emphasis = paragraph.contents[0] if len(paragraph.contents) == 1 else None
        strong = emphasis.contents[0] if getattr(emphasis, 'name', None) == 'em' and len(emphasis.contents) == 1 else None
        if (len(sources) != 1 or paragraph.attrs or getattr(strong, 'name', None) != 'strong'
                or emphasis.attrs or strong.attrs or len(strong.contents) != 1
                or not isinstance(strong.contents[0], NavigableString) or isinstance(strong.contents[0], Comment)
                or re.fullmatch(r'文章來源:\s+[^\r\n]{1,120} - [^\r\n]{1,64}分析師', str(strong.contents[0])) is None
                or any(isinstance(node, Comment) for node in figure.descendants)):
            return None
        return (figure.get_text('\n', strip=True), (tuple(sources), ()),
                'same_article_image_figure_source_credit')
    if [node.name for node in elements] == ['img', 'strong', 'strong']:
        # Actual 5007990: the publisher places a numbered subsection heading
        # inside the same image figure, not a risk disclaimer or figcaption.
        # Its exact two visible/current text nodes and presentation style must
        # agree; the caller still matches full schema, image, order and scope.
        # Preserve the heading separately, never treat its image alt as text.
        headings = elements[1:]
        style = {'style': 'letter-spacing:0.7px; text-align:justify'}
        if (len(sources) != 1 or any(node.attrs != style or len(node.contents) != 1
                or not isinstance(node.contents[0], NavigableString)
                or isinstance(node.contents[0], Comment) or not node.get_text(strip=True) for node in headings)
                or not re.fullmatch(r'[1-9][0-9]{0,2}[.]', str(headings[0].contents[0]))
                or any(isinstance(node, Comment) for node in figure.descendants)):
            return None
        return (figure.get_text('\n', strip=True), (tuple(sources), ()),
                'same_article_image_figure_numbered_heading')
    if [node.name for node in elements] == ['figure', 'figcaption']:
        caption = elements[1]
        if (caption.attrs or len(caption.contents) != 1
                or not isinstance(caption.contents[0], NavigableString)
                or isinstance(caption.contents[0], Comment) or not caption.get_text(strip=True)):
            return None
        return caption.get_text(' ', strip=True), sources[0], 'nested_caption'
    names = ','.join(node.name for node in elements)
    nested = bool(children)
    pattern = r'(?:img,)?(?:p,){0,4}(?:figure,){1,8}p' if nested else r'img,(?:p,){1,4}p'
    if re.fullmatch(pattern, names) is None:
        return None
    warning = elements[-1]
    if (warning.attrs != {'class': ['warning']} or len(warning.find_all('br')) != 2
            or any(node.name != 'br' or node.attrs for node in warning.find_all(True))
            or warning.get_text('\n', strip=True) != '\n'.join(_CNYES_INVESTMENT_NOTICE_LINES)):
        return None
    links = []
    for paragraph in (node for node in elements[:-1] if node.name == 'p'):
        if (paragraph.attrs or not paragraph.get_text(strip=True)
                or any(node.name not in {'a', 'br'} for node in paragraph.find_all(True))
                or any(isinstance(node, Comment) for node in paragraph.descendants)):
            return None
        for link in paragraph.find_all('a'):
            href = link.get('href')
            if not isinstance(href, str) or link.find(True) or not link.get_text(strip=True):
                return None
            try:
                parts = urlsplit(href)
            except ValueError:
                return None
            if parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username or parts.password:
                return None
            links.append((href, link.get_text(' ', strip=True)))
    if any(isinstance(node, Comment) for node in warning.descendants):
        return None
    return (figure.get_text('\n', strip=True), (tuple(sources), tuple(links)),
            'same_article_nested_figure_text' if nested else 'same_article_image_figure_text')


class _CnyesCaptionView(NamedTuple):
    """Complete schema comparison and independently preserved narrative."""
    comparison: Any
    captions: list[dict[str, Any]]
    notices: list[dict[str, Any]]
    empty_caption_placeholders: int
    empty_figure_placeholders: int
    narrative_body: str | None = None
    narrative_kinds: tuple[str, ...] = ()


def _cnyes_caption_schema_view(container, full_content, source, excluded, revealed_wrapper):
    """Match current-content figure captions to actual visible figures.

    Build a comparison view only. Never use a cover/related image, download an
    image, supply hidden text or change the preserved narrative. Exact image
    src, caption, unique membership, order and full text must corroborate.
    Public responses 5142712/5008397 also contain a separate caption-free
    image figure. Match that exact textless image too; it neither invalidates
    another real caption nor supplies caption/body text. Healthy matching
    narrative still avoids this comparison-only reparse entirely. 4949007's
    same-image figure contains an exact investment-risk disclaimer rather
    than a figcaption; retain that proven notice separately, not as a caption
    or invented article narrative. 4636131 also retains one same-position
    textless figure with an empty NBSP caption in both native/current DOMs.
    Match that exact placeholder, never a missing image or narrative caption.
    4790747/5041298/4852826 retain bounded nested image-only leaves beneath a
    caption or publisher supplement. Match the complete forest once, preserve
    every source link/image reference and text separately, and still require
    whole current/visible/schema agreement. Unknown nesting fails closed.
    4822498's direct-image supplement follows that same proof, but retains a
    distinct structural kind instead of falsely calling it nested or a caption.
    """
    from bs4 import BeautifulSoup, NavigableString, Comment
    from downloader.news_fulltext_policy import _plain_text_node

    # 4470101 retains distinct bare-NBSP and NBSP-caption figures. Their
    # exact unique current/visible membership and order still need proof.
    current = BeautifulSoup(html.unescape(full_content), source.html_parser)
    all_current_figures = current.find_all('figure')
    all_visible_figures = container.find_all('figure')
    if (not all_current_figures or len(all_current_figures) > 32 or len(all_visible_figures) > 33):
        return None
    # Match roots once, not both a wrapper and its child image. Keep the bound
    # on every figure, including descendants, so nesting cannot evade it.
    current_figures = [node for node in all_current_figures if not node.find_parent('figure')]
    visible_figures = [node for node in all_visible_figures if not node.find_parent('figure')]

    def structured(figure):
        return (_cnyes_structured_figure_identity(figure)
            or _cnyes_image_paragraph_figure_identity(figure)
            or _cnyes_ordered_figure_forest_identity(figure)
            or _cnyes_matched_figure_paragraph_identity(figure))

    def identity(figure, reveal, *, verify_visibility=True):
        if figure.find('figcaption') and not figure.find('img'):
            key = _cnyes_ordered_figure_forest_identity(figure)
            if key is not None:
                if verify_visibility:
                    for node in (figure, *figure.find_all(True)):
                        _cnyes_visible_after_completed_stream(node, reveal)
                return key
        empty_chart = _cnyes_empty_chart_caption_reference(figure)
        if empty_chart is not None:
            if verify_visibility:
                for node in (figure, *figure.find_all(True)):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return empty_chart
        chart = _cnyes_typed_chart_caption_reference(figure)
        if chart is not None:
            if verify_visibility:
                for node in (figure, *figure.find_all(True)):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return chart
        if figure.find('figure') is not None:
            key = structured(figure)
            if key is not None and verify_visibility:
                for node in (figure, *figure.find_all(True)):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return key
        if figure.find('a') is not None:
            # Textless linked images also need href/DOM identity. Never fall
            # back to a bare image when this typed anchor structure rejects.
            key = structured(figure)
            if key is not None and verify_visibility:
                for node in (figure, *figure.find_all(True)):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return key
        captions, images = figure.find_all('figcaption'), figure.find_all('img')
        if (not figure.attrs and len(figure.contents) == 1
                and isinstance(figure.contents[0], NavigableString)
                and not isinstance(figure.contents[0], Comment)
                and str(figure.contents[0]) == '\xa0'):
            if verify_visibility:
                _cnyes_visible_after_completed_stream(figure, reveal)
            return '', '', 'empty_figure_placeholder'
        if (not figure.attrs and not images and len(captions) == 1
                and captions[0].parent is figure and not captions[0].attrs
                and len(captions[0].contents) == 1
                and isinstance(captions[0].contents[0], NavigableString)
                and not isinstance(captions[0].contents[0], Comment)
                and str(captions[0].contents[0]) == '\xa0'
                and all(child is captions[0] or isinstance(child, NavigableString)
                        and not isinstance(child, Comment) and not str(child).strip()
                        for child in figure.contents)):
            if verify_visibility:
                for node in (figure, captions[0]):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return '', '', 'empty_caption_placeholder'
        if not images and not captions:
            key = structured(figure)
            if key is not None and verify_visibility:
                for node in (figure, *figure.find_all(True)):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return key
        if (figure.find('figure') or len(captions) > 1
                or len(images) != 1 or not isinstance(images[0].get('src'), str)):
            return None
        caption = captions[0] if captions else None
        text = caption.get_text(' ', strip=True) if caption is not None else ''
        kind = 'caption' if caption is not None else 'textless_image'
        if caption is None and figure.get_text(' ', strip=True):
            text = _cnyes_figure_notice_text(figure)
            if text is None:
                key = structured(figure)
                if key is not None and verify_visibility:
                    for node in (figure, *figure.find_all(True)):
                        _cnyes_visible_after_completed_stream(node, reveal)
                return key
            kind = 'same_article_investment_risk_disclaimer'
        image_src = images[0]['src']
        image_url = urlsplit(image_src)
        if (caption is not None and (caption.parent is not figure or not text)
                or re.sub(r'\s+', '', figure.get_text(' ', strip=True)) != re.sub(r'\s+', '', text)
                or image_url.scheme not in {'http', 'https'} or not image_url.hostname
                or image_url.username or image_url.password):
            key = structured(figure)
            if key is not None and verify_visibility:
                for node in (figure, *figure.find_all(True)):
                    _cnyes_visible_after_completed_stream(node, reveal)
            return key
        nodes = [figure, images[0]]
        if caption is not None:
            nodes.extend([caption, *caption.find_all(True)])
        if kind == 'same_article_investment_risk_disclaimer':
            nodes.extend(figure.find_all(True))
        if verify_visibility:
            for node in nodes:
                _cnyes_visible_after_completed_stream(node, reveal)
        return text, image_src, kind

    current_keys = [identity(figure, None) for figure in current_figures]
    if any(key is None for key in current_keys) or len(set(current_keys)) != len(current_keys):
        return None
    wanted = set(current_keys)
    visible_keys = []
    for figure in visible_figures:
        # An unrelated cover stays excluded, as it is in the narrative path.
        # Only a same-src/caption current figure can enter this new proof.
        key = identity(figure, None, verify_visibility=False)
        if key is None and figure.find('figure') is not None:
            return None  # Unknown nesting is not an independently verified cover.
        if key is not None and key[2] in {'empty_caption_placeholder', 'empty_figure_placeholder'} and key not in wanted:
            return None  # A textless template is not an independently verified cover.
        visible_keys.append(identity(figure, revealed_wrapper) if key in wanted else None)
    selected_keys = [key for key in visible_keys if key is not None]
    if selected_keys != current_keys:
        return None
    comparison = BeautifulSoup(str(container), source.html_parser)
    comparison_roots = [node for node in comparison.find_all('figure') if not node.find_parent('figure')]
    narrative = None
    narrative_kinds = tuple(sorted({key[2] for key in current_keys
        if key[2] in {'same_article_caption_gallery_narrative_tail', 'same_article_image_leaf_prose_figure',
                     'same_article_image_paragraph_narrative', 'same_article_ordered_image_leaf_paragraphs',
                     'same_article_matched_figure_paragraphs'}}))
    if narrative_kinds:
        # Copy once only on this repaired failure path. Do not reparse/walk the
        # whole article per figure. Retain the actual visible p at its original
        # article position, while image/caption metadata stays separate.
        from copy import deepcopy
        narrative = deepcopy(comparison)
        narrative_roots = [node for node in narrative.find_all('figure') if not node.find_parent('figure')]
        for figure, key in zip(narrative_roots, visible_keys, strict=True):
            if key is not None and key[2] == 'same_article_matched_figure_paragraphs':
                for caption in figure.find_all('figcaption'):
                    caption.decompose()  # Explicit captions remain paired metadata, never narrative.
                for nested in reversed(figure.find_all('figure')):
                    nested.unwrap()
                figure.unwrap()
                continue
            if key is not None and key[2] in {'same_article_image_leaf_prose_figure', 'same_article_image_paragraph_narrative',
                                             'same_article_ordered_image_leaf_paragraphs'}:
                figure.unwrap()
                continue
            if key is not None and key[2] == 'same_article_caption_gallery_narrative_tail':
                tail = figure.find('figcaption', recursive=False).find_all(True, recursive=False)[-1]
                figure.replace_with(tail.extract())
            figure.decompose()
    for figure, key in zip(comparison_roots, visible_keys, strict=True):
        if key is not None:
            figure.replace_with(NavigableString(key[0]))
        else:
            figure.decompose()
    for figure, key in zip(current_figures, current_keys, strict=True):
        figure.replace_with(NavigableString(key[0]))
    compact = lambda text: re.sub(r'\s+', '', text)
    if compact(_plain_text_node(comparison, excluded, parser=source.html_parser)) != compact(
            _plain_text_node(current, excluded, parser=source.html_parser)):
        return None
    # A gallery contributes individual source caption/image pairs, never one
    # falsely concatenated caption assigned to the outer image.
    # One source-order pass, not simple captions first and galleries later.
    # Actual4631268 has both kinds; grouping by kind reordered its image pairs.
    captions = []
    for text, value, kind in current_keys:
        if kind in {'caption', 'nested_caption'}:
            captions.append({'text': text, 'image_src': value,
                **({'structure_basis': 'same_article_nested_image_figure_and_caption'} if kind == 'nested_caption' else {})})
        elif kind in {'same_article_caption_gallery', 'same_article_caption_gallery_narrative_tail'}:
            captions.extend({'text': text, 'image_src': src,
                             'structure_basis': ('same_article_visible_nested_captions_and_source_image_order'
                                                 if kind == 'same_article_caption_gallery_narrative_tail' else
                                                 'same_article_visible_caption_gallery_and_source_image_order')}
                            for text, src in value[1])
        elif kind == 'same_article_matched_figure_paragraphs':
            captions.extend({'text': text, 'image_src': src,
                'structure_basis': 'same_article_matched_figure_explicit_caption_and_image'}
                for text, src in value[3])
        elif kind in {'same_article_linked_caption_figure', 'same_article_caption_leaf_placeholder',
                      'same_article_typed_chart_caption_reference', 'same_article_labelled_image_paragraph_caption'}:
            captions.extend({'text': text, 'image_src': src,
                             'structure_basis': ('same_article_visible_linked_caption_and_source_image_order'
                                                 if kind == 'same_article_linked_caption_figure' else
                                                 'same_article_typed_chart_label_and_source_image'
                                                 if kind == 'same_article_typed_chart_caption_reference' else
                                                 'same_article_visible_labelled_image_paragraph_and_source_image'
                                                 if kind == 'same_article_labelled_image_paragraph_caption' else
                                                 'same_article_visible_caption_leaf_and_placeholder_order')}
                            for text, src in value[3])
    # A genuinely absent caption is not an empty invented caption record.
    return _CnyesCaptionView(comparison,
            captions,
            [{'text': '' if kind in {'same_article_image_leaf_prose_figure', 'same_article_linked_caption_figure',
                                    'same_article_caption_leaf_placeholder', 'same_article_typed_chart_caption_reference',
                                    'same_article_image_paragraph_narrative', 'same_article_labelled_image_paragraph_caption',
                                    'same_article_ordered_image_leaf_paragraphs',
                                    'same_article_matched_figure_paragraphs'} else text,
              'kind': kind,
              **({'image_sources': list(src[0]), 'source_links': [{'href': href, 'text': label} for href, label in src[1]]}
                 if kind in {'same_article_nested_figure_text', 'same_article_nested_mixed_figure_text', 'same_article_image_figure_text',
                             'same_article_image_figure_numbered_heading', 'same_article_image_figure_source_credit',
                             'same_article_image_figure_link_text', 'same_article_linked_image_reference',
                             'same_article_image_figure_one_line_warning_supplement', 'same_article_image_figure_social_links',
                             'same_article_empty_chart_caption_reference', 'same_article_figure_source_byline',
                             'same_article_typed_chart_caption_reference',
                             'same_article_linked_caption_figure', 'same_article_warning_placeholder_figure',
                             'same_article_image_leaf_prose_figure', 'same_article_image_figure_social_placeholder_links',
                             'same_article_caption_leaf_placeholder', 'same_article_image_paragraph_narrative',
                             'same_article_labelled_image_paragraph_caption'} or kind in _CNYES_ORDERED_FIGURE_KINDS
                 else {'image_src': src}),
              **({'source_dom_sha256': src[2]} if kind in _CNYES_ORDERED_FIGURE_KINDS else {})}
             for text, src, kind in current_keys
             if kind in {'same_article_investment_risk_disclaimer', 'same_article_nested_figure_text', 'same_article_nested_mixed_figure_text',
                         'same_article_image_figure_text', 'same_article_image_figure_numbered_heading',
                         'same_article_image_figure_source_credit', 'same_article_image_figure_link_text',
                         'same_article_linked_image_reference', 'same_article_image_figure_one_line_warning_supplement',
                         'same_article_image_figure_social_links',
                         'same_article_empty_chart_caption_reference', 'same_article_figure_source_byline',
                         'same_article_typed_chart_caption_reference',
                         'same_article_linked_caption_figure', 'same_article_warning_placeholder_figure',
                         'same_article_image_leaf_prose_figure', 'same_article_image_figure_social_placeholder_links',
                         'same_article_caption_leaf_placeholder', 'same_article_image_paragraph_narrative',
                         'same_article_labelled_image_paragraph_caption'} or kind in _CNYES_ORDERED_FIGURE_KINDS],
            sum(kind == 'empty_caption_placeholder' for _, _, kind in current_keys),
            sum(kind == 'empty_figure_placeholder' for _, _, kind in current_keys),
            _plain_text_node(narrative, excluded, parser=source.html_parser) if narrative is not None else None,
            narrative_kinds)


def cnyes_supported_mobile_view(parts):
    """Only the observed URI shape; never infer arbitrary query semantics.

    Preserve exp=a/exp=b in the requested/final URL and raw response. Acceptance
    still requires the actual page's canonical/current/schema/body proof;
    this predicate alone declares neither equivalence nor a public body.
    """
    return (parts.scheme == 'https' and parts.netloc == 'm.cnyes.com'
            and re.fullmatch(r'/news/id/[0-9]+', parts.path) is not None
            and parts.query in {'', 'exp=a', 'exp=b'} and not parts.fragment)


def cnyes_supported_native_alias_view(parts):
    """Observed mobile variants and the exact desktop exp=a representation."""
    return cnyes_supported_mobile_view(parts) or (
        parts.scheme == 'https' and parts.netloc == 'news.cnyes.com'
        and re.fullmatch(r'/news/id/[0-9]+', parts.path) is not None
        and parts.query == 'exp=a' and not parts.fragment)


@dataclass(frozen=True)
class _CnyesNativeIdentity:
    article: dict
    current_data: dict
    container: Any
    revealed_wrapper: Any
    visible_title: str
    requested_parts: SplitResult
    visible_byline: str
    author_names: list[str]
    declared_url: str


def _cnyes_native_article_identity(soup, source, *, page_url):
    """Corroborate native identity/rights/byline/clocks, independently of body.

    No generated redirect, second request, or metadata from an adjacent story.
    Shared by body acceptance and bounded private failed-response diagnosis.
    A successful identity witness does NOT accept an article or its body.
    """
    from downloader.news_fulltext_policy import _objects, normalize_url

    parts = urlsplit(page_url or '')
    match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
    if (source.provider != 'cnyes' or not match
            or not (parts.hostname == 'm.cnyes.com'
                    or parts.hostname == 'news.cnyes.com' and parts.query == 'exp=a')):
        return None
    expected = normalize_url(page_url)
    canonical = _select_nodes(soup, 'link[rel="canonical"][href]')
    if len(canonical) != 1:
        raise NewsError('article_identity_not_verified')
    declared_url = normalize_url(canonical[0]['href'])
    if declared_url == expected:
        return None
    declared_parts = urlsplit(declared_url)
    if (not cnyes_supported_native_alias_view(parts)
            or declared_parts.scheme != 'https' or declared_parts.netloc != 'news.cnyes.com'
            or declared_parts.path != parts.path or declared_parts.query or declared_parts.fragment):
        raise NewsError('article_identity_not_verified')
    og = _select_nodes(soup, 'meta[property="og:url"][content]')
    current = _cnyes_visible_article_scope(soup)
    if len(og) != 1 or normalize_url(og[0]['content']) != declared_url or current is None:
        raise NewsError('article_identity_not_verified')
    header, container = current
    parent = container.find_parent('article')
    reveal = _cnyes_completed_stream_wrapper(soup, parent)
    for node in (header, container):
        _cnyes_visible_after_completed_stream(node, reveal)
    data = _cnyes_current_article_data(soup, int(match[1]), verify_author=True)
    visible_title = header.get_text(' ', strip=True)
    if (data is None or normalize_url(data.get('url', '')) != declared_url
            or not cnyes_visible_headline_matches(visible_title, data.get('title'))):
        raise NewsError('article_identity_not_verified')
    if type(data.get('payment')) is int and data['payment'] > 0:
        raise NewsError('paywall_or_login')
    if type(data.get('payment')) is not int or data['payment'] != 0:
        raise NewsError('article_identity_not_verified')
    candidates = []
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            for candidate in _objects(json.loads(script.get_text())):
                if candidate.get('@type') != 'NewsArticle' or candidate.get('url') != declared_url:
                    continue
                identity = candidate.get('mainEntityOfPage')
                identity = identity.get('@id') if isinstance(identity, dict) else identity
                if identity != declared_url or candidate.get('headline') != data['title']:
                    raise NewsError('article_identity_not_verified')
                if str(candidate.get('isAccessibleForFree', 'true')).lower() == 'false':
                    raise NewsError('paywall_or_login')
                candidates.append(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
    if len(candidates) != 1:
        raise NewsError('article_identity_not_verified')
    article = candidates[0]
    clocks = parent.select(':scope > p > time[datetime]')
    try:
        clock = datetime.fromisoformat(article['datePublished'].replace('Z', '+00:00'))
        if (clock.tzinfo is None or type(data.get('publishAt')) is not int
                or clock.timestamp() != data['publishAt'] or len(clocks) != 1
                or clocks[0]['datetime'] != article['datePublished']):
            raise ValueError('source clock conflict')
        _cnyes_visible_after_completed_stream(clocks[0], reveal)
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        raise NewsError('invalid_bulk_publication_clock') from None
    authors = article.get('author')
    authors = authors if isinstance(authors, list) else [authors]
    names = [a.get('name') for a in authors if isinstance(a, dict)]
    bylines = parent.select(':scope > p > .signature')
    if (len(names) != 1 or not isinstance(names[0], str) or not names[0].strip()
            or names[0] != data.get('author') or len(bylines) != 1):
        raise NewsError('article_attribution_not_verified')
    _cnyes_visible_after_completed_stream(bylines[0], reveal)
    visible_byline = bylines[0].get_text(' ', strip=True)
    if not visible_byline:
        # Observed same-response public article 4955510: an explicitly empty
        # signature span, but matching named authors in the current NewsArticle
        # and current-ID data. Preserve the actual blank visible credit; never
        # borrow a neighbouring author or claim the name was visible. Unknown
        # descendants, accessibility labels and hidden signatures stay gaps.
        if (bylines[0].name != 'span' or bylines[0].get('class') != ['signature']
                or any(key in bylines[0].attrs for key in ('title', 'aria-label', 'aria-labelledby'))
                or any(not isinstance(node, str) or str(node).strip() for node in bylines[0].contents)):
            raise NewsError('article_attribution_not_verified')
    return _CnyesNativeIdentity(article, data, container, reveal, visible_title, parts,
                               visible_byline, names, declared_url)


def _cnyes_legacy_template_reference_witness(container, full_content, source, excluded, reveal, current_text):
    """Match retained190469/232219's leaked legacy reference comments.

    Keep the original visible text and whole schema unchanged. Only compare
    the same ordered source reference tokens against original current comments;
    every remaining word must agree. Never supply image pixels, table values,
    hidden text or a general comment/markup normalization.
    """
    from bs4 import BeautifulSoup, NavigableString, Comment
    from downloader.news_fulltext_policy import _plain_text_node

    markers = container.find_all(['article_table', 'article_image'])
    if not current_text.strip() or not 1 <= len(markers) <= 16:
        return None
    current = BeautifulSoup(html.unescape(full_content), source.html_parser)
    comments = current.find_all(string=lambda node: isinstance(node, Comment))
    if len(comments) != len(markers):
        return None
    references = []
    for marker, comment in zip(markers, comments, strict=True):
        if (marker.attrs or marker.parent.name != 'p' or comment.parent.name != 'p'
                or len(marker.contents) != 1 or not isinstance(marker.contents[0], NavigableString)
                or isinstance(marker.contents[0], Comment)):
            return None
        label = str(marker.contents[0])
        pattern = (r'[0-9]{1,3}[.]table' if marker.name == 'article_table'
                   else r'[0-9]{8}_[0-9]{1,6}_[0-9]{1,4}[.]jpg')
        suffix = marker.next_sibling
        if (re.fullmatch(pattern, label) is None
                or str(comment).strip() != f'<{marker.name}>{label}</{marker.name}>'
                or not isinstance(suffix, NavigableString) or isinstance(suffix, Comment)
                or not str(suffix).startswith(' -->')):
            return None
        _cnyes_visible_after_completed_stream(marker, reveal)
        references.append({'text': label + ' -->', 'kind': 'same_article_legacy_template_reference',
            'marker_kind': marker.name, 'source_reference': label, 'referenced_asset_or_values_reconstructed': False})
    comparison = BeautifulSoup(str(container), source.html_parser)
    for marker in comparison.find_all(['article_table', 'article_image']):
        suffix = marker.next_sibling
        if not isinstance(suffix, NavigableString) or not str(suffix).startswith(' -->'):
            return None
        suffix.replace_with(NavigableString(str(suffix)[4:]))
        marker.decompose()
    compact = lambda text: re.sub(r'\s+', '', text)
    if compact(_plain_text_node(comparison, excluded, parser=source.html_parser)) != compact(current_text):
        return None
    return references


def _cnyes_schema_legacy_comment_witness(container, full_content, source, excluded, reveal, schema_views):
    """Corroborate schema-only legacy image-reference residue, not hidden prose.

    Retained 2013 native pages have the same ordered image comment in current
    content and in the section renderer. Only JSON-LD exposes its filename and
    closing comment marker as text. Reconstruct a comparison view from BOTH
    complete DOMs at the actual comment positions; preserve the real visible
    body and references separately. Unknown comments/markup, extra text, hidden
    parents, duplicate references and mismatching schema remain failures.
    Healthy pages never enter this mismatch-only path.
    """
    from bs4 import BeautifulSoup, Comment, NavigableString
    from downloader.news_fulltext_policy import _plain_text_node

    if (not isinstance(full_content, str)
            or not any(marker in full_content for marker in ('<article_image>', '&lt;article_image&gt;'))):
        return None
    current = BeautifulSoup(html.unescape(full_content), source.html_parser)
    if current.head is not None or current.find('figure') is not None:
        return None
    current_root = current.body or current
    current_comments = current_root.find_all(string=lambda node: isinstance(node, Comment))
    visible_comments = container.find_all(string=lambda node: isinstance(node, Comment))
    if not 1 <= len(current_comments) <= 16 or len(current_comments) != len(visible_comments):
        return None
    references = []
    labels = []
    for comment, rendered in zip(current_comments, visible_comments, strict=True):
        match = re.fullmatch(r'<article_image>([0-9]{8}_[0-9]{1,6}_[0-9]{1,4}[.]jpg)</article_image> ', str(comment))
        if (match is None or comment.parent is not current_root
                or rendered.parent.name != 'section' or rendered.parent.attrs != {'style': 'margin-top:30px'}):
            return None
        label = match[1]
        renderer = ('</section><section style="margin-top:30px"><article_image>' + label
                    + '</article_image></section><section style="margin-top:30px"> ')
        if str(rendered) != renderer:
            return None
        _cnyes_visible_after_completed_stream(rendered.parent, reveal)
        labels.append(label)
        references.append({'text': label + ' -->', 'kind': 'same_article_schema_legacy_comment_reference',
            'marker_kind': 'article_image', 'source_reference': label,
            'referenced_asset_or_values_reconstructed': False,
            'visible_as_narrative': False})
    if len(set(labels)) != len(labels):
        return None
    # Two whole ordered comparisons, not a substring deletion in the schema.
    # Preserve both original trees; only these temporary comparison copies
    # reveal the independently matched reference text, never hidden prose.
    comparison = BeautifulSoup(str(container), source.html_parser)
    cloned_comments = comparison.find_all(string=lambda node: isinstance(node, Comment))
    if len(cloned_comments) != len(labels):
        return None
    for comment, rendered, label in zip(current_comments, cloned_comments, labels, strict=True):
        comment.replace_with(NavigableString(label + ' -->'))
        rendered.replace_with(NavigableString(label + ' -->'))
    compact = lambda text: re.sub(r'\s+', '', text)
    visible_view = compact(_plain_text_node(comparison, excluded, parser=source.html_parser))
    current_view = compact(_plain_text_node(current_root, excluded, parser=source.html_parser))
    if not visible_view or visible_view != current_view or visible_view not in schema_views:
        return None
    return visible_view, references


def _cnyes_narrative_figure_views(container, full_content, source, revealed_wrapper):
    """Recover the retained 4898771 narrative wrapper, not arbitrary captions.

    The publisher wraps thirteen prose/disclosure paragraphs and two image-only
    leaves in one attribute-free figure. The usual figure exclusion therefore
    removes the entire article. Match the actual ordered DOM to current free
    content, validate every node's visibility, then unwrap ONLY that root in
    private comparison copies. Image leaves, captions and normal hot paths keep
    their existing exclusions. The caller still requires the complete schema.
    No media request, hidden text, generic figure flattening or source mutation.
    """
    from copy import deepcopy
    from bs4 import BeautifulSoup, Comment, NavigableString

    current = BeautifulSoup(html.unescape(full_content), source.html_parser)
    current_roots = [node for node in current.find_all('figure') if not node.find_parent('figure')]
    visible_roots = [node for node in container.find_all('figure') if not node.find_parent('figure')]
    if len(current_roots) != 1 or len(visible_roots) != 1:
        return None
    declared, visible = current_roots[0], visible_roots[0]
    elements = declared.find_all(True, recursive=False)
    if (declared.attrs or str(declared) != str(visible)
            or [node.name for node in elements] != ['figure', 'p', 'p', 'figure', *(['p'] * 11)]
            or any(isinstance(node, Comment) for node in declared.descendants)
            or any(isinstance(node, NavigableString) and str(node).strip() for node in declared.contents)):
        return None
    leaves = [elements[0], elements[3]]
    for leaf in leaves:
        children = leaf.find_all(True, recursive=False)
        if (leaf.attrs or len(children) != 1 or children[0].name != 'img'
                or len(leaf.find_all(True)) != 1
                or any(isinstance(node, NavigableString) and str(node).strip() for node in leaf.contents)):
            return None
    if _cnyes_typed_figure_images(declared) is None:
        return None
    paragraphs = [node for node in elements if node.name == 'p']
    warning = paragraphs[-1]
    blank = paragraphs[-2]
    if (warning.attrs != {'class': ['warning']} or len(warning.find_all('br')) != 4
            or len(warning.find_all('u')) != 1
            or blank.attrs or len(blank.contents) != 1
            or not isinstance(blank.contents[0], NavigableString) or str(blank.contents[0]) != '\xa0'
            or any(node.attrs or not node.get_text(strip=True) for node in paragraphs[:-2])
            or any(node.name not in {'strong', 'a', 'br', 'u'} for node in declared.find_all(True)
                   if node.find_parent('p') is not None)):
        return None
    for paragraph in paragraphs:
        if paragraph is blank:
            continue  # The original has one exact penultimate bare-NBSP spacer.
        if not paragraph.get_text(strip=True):
            return None
        for node in paragraph.find_all(True):
            if node.name == 'a':
                if (set(node.attrs) != {'href'} or not _cnyes_public_figure_url(node['href'])
                        or node.find(True) is not None or not node.get_text(strip=True)):
                    return None
            elif node.attrs or (node.name == 'br' and node.contents):
                return None
            elif node.name == 'u' and node.parent is not warning:
                return None
    for node in (visible, *visible.find_all(True)):
        _cnyes_visible_after_completed_stream(node, revealed_wrapper)
    # Exactly one outer root is present in each copy, and only it is removed.
    # The image-only leaves remain ordinary excluded figures.
    comparison = deepcopy(container)
    comparison.find('figure').unwrap()
    declared.unwrap()
    return comparison, current


def _cnyes_native_body_failure(stage):
    """Static, text-free failure provenance; same error/state/retry semantics."""
    if stage not in CNYES_NATIVE_BODY_FAILURE_STAGES:
        raise ValueError('unknown_cnyes_native_body_failure_stage')
    error = NewsError('body_not_found')
    error.cnyes_native_body_failure_stage = stage
    return error


def cnyes_native_canonical_article(soup, source, *, page_url):
    """Accept a native alias only with its complete current visible body proof.

    Original-host extraction keeps its existing fast path. Identity alone is
    never enough: whole body, figures, source numbers and visibility must agree.
    """
    from downloader.news_fulltext_policy import _plain_text, _plain_text_node

    identity = _cnyes_native_article_identity(soup, source, page_url=page_url)
    if identity is None:
        return None
    article, data = identity.article, identity.current_data
    container, reveal = identity.container, identity.revealed_wrapper
    visible_title, parts = identity.visible_title, identity.requested_parts
    # Source ad skeletons contain U+200C. Exclude only this registered ad slot;
    # do not strip zero-width characters from real words or financial text.
    excluded = (*source.exclude_selectors, '[id^="div-gpt-ad-cnyes_news_article_"]')
    visible = _plain_text_node(container, excluded, parser=source.html_parser)
    compact = lambda text: re.sub(r'\s+', '', text)
    schema_body = article.get('articleBody')
    full_content = data.get('content')
    verified_figure_narrative_body = False
    figure_current_text = None
    verified_caption_gallery_text_body = False
    verified_caption_wrapped_narrative = False
    verified_image_leaf_prose_narrative = False
    verified_image_paragraph_narrative = False
    verified_ordered_image_leaf_paragraphs = False
    verified_matched_figure_paragraphs = False
    caption_only_view = None
    if not visible and isinstance(schema_body, str) and isinstance(full_content, str) and container.find('figure'):
        views = _cnyes_narrative_figure_views(container, full_content, source, reveal)
        if views is not None:
            visible = _plain_text_node(views[0], excluded, parser=source.html_parser)
            figure_current_text = _plain_text_node(views[1], excluded, parser=source.html_parser)
            verified_figure_narrative_body = True
    if not visible and isinstance(schema_body, str) and isinstance(full_content, str) and container.find('figure'):
        # 4795333 is entirely original photo captions. The ordinary narrative
        # path correctly excludes figures, but that is not evidence of an
        # absent textual article. Require the bounded gallery grammar, exact
        # image/caption/DOM order, visibility, complete current text and whole
        # schema before retaining these captions as an explicitly typed body.
        view = _cnyes_caption_schema_view(container, full_content, source, excluded, reveal)
        if view is not None and (view.narrative_body is not None or any(caption.get('structure_basis') ==
                'same_article_visible_caption_gallery_and_source_image_order' for caption in view.captions)):
            gallery_text = _plain_text_node(view[0], excluded, parser=source.html_parser)
            if gallery_text and compact(gallery_text) in {
                    compact(schema_body), compact(_plain_text(schema_body, parser=source.html_parser))}:
                visible = view.narrative_body if view.narrative_body is not None else gallery_text
                caption_only_view = view
                verified_caption_wrapped_narrative = 'same_article_caption_gallery_narrative_tail' in view.narrative_kinds
                verified_image_leaf_prose_narrative = 'same_article_image_leaf_prose_figure' in view.narrative_kinds
                verified_image_paragraph_narrative = 'same_article_image_paragraph_narrative' in view.narrative_kinds
                verified_ordered_image_leaf_paragraphs = 'same_article_ordered_image_leaf_paragraphs' in view.narrative_kinds
                verified_matched_figure_paragraphs = 'same_article_matched_figure_paragraphs' in view.narrative_kinds
                verified_caption_gallery_text_body = view.narrative_body is None
    if not visible or not isinstance(schema_body, str) or not isinstance(full_content, str):
        if not visible and container.find('img') and cnyes_verified_image_only_body(
                soup, source, article, page_url=page_url, identity=identity):
            raise NewsError('source_body_image_only')
        raise _cnyes_native_body_failure('missing_body_representation')
    current_text = (figure_current_text if verified_figure_narrative_body else
                    _plain_text(html.unescape(full_content), parser=source.html_parser))
    verified_legacy_template_references = []
    if not (verified_caption_gallery_text_body or verified_caption_wrapped_narrative
            or verified_image_leaf_prose_narrative or verified_image_paragraph_narrative
            or verified_ordered_image_leaf_paragraphs or verified_matched_figure_paragraphs) and compact(visible) != compact(current_text):
        legacy = _cnyes_legacy_template_reference_witness(container, full_content, source, excluded, reveal, current_text)
        if legacy is None:
            raise _cnyes_native_body_failure('visible_current_mismatch')
        verified_legacy_template_references = legacy
    schema_compared_paragraph_markers = False
    schema_compared_em_paragraph_markers = False
    schema_compared_anchor_markers = False
    schema_compared_factset_markers = False
    schema_compared_stock_list_markers = False
    verified_captions = []
    verified_notices = []
    verified_empty_caption_placeholders = 0
    verified_empty_figure_placeholders = 0
    verified_schema_legacy_comment_references = []
    if caption_only_view is not None:
        (_, verified_captions, verified_notices,
         verified_empty_caption_placeholders, verified_empty_figure_placeholders) = caption_only_view[:5]
    # JSON-LD articleBody is Text, not necessarily HTML. The actual 4968333
    # declares literal <Line@...> text that its escaped current/visible DOM
    # preserves; treating that literal as a tag deletes real source words.
    # Compare each entire representation exactly, never strip selected words
    # or accept a schema-only body. Cache the parsed view once per article.
    literal_schema = compact(schema_body)
    parsed_schema = compact(_plain_text(schema_body, parser=source.html_parser))
    schema_views = {literal_schema, parsed_schema}
    compared_text = compact(_plain_text_node(caption_only_view.comparison, excluded, parser=source.html_parser)
                            if verified_caption_wrapped_narrative or verified_image_leaf_prose_narrative
                            or verified_image_paragraph_narrative or verified_ordered_image_leaf_paragraphs
                            or verified_matched_figure_paragraphs else visible)
    if compared_text not in schema_views:
        # Observed public HTML/current content retain a leading U+FEFF in some
        # paragraphs; its own JSON-LD omits it. Compare only that exact DOM
        # formatting case. Preserve the original visible body, every in-word
        # marker and all numerical values; current free content must still
        # match the visible text WITHOUT this relaxation.
        from bs4 import BeautifulSoup
        comparison = BeautifulSoup(str(container), source.html_parser)
        removed = _cnyes_remove_leading_paragraph_markers(comparison)
        em_removed = _cnyes_remove_verified_em_paragraph_markers(comparison, full_content, source)
        anchor_removed = _cnyes_remove_verified_anchor_markers(comparison, full_content, source)
        factset_removed = _cnyes_remove_verified_factset_markers(comparison, full_content, source)
        stock_list_removed = _cnyes_remove_verified_stock_list_markers(comparison, full_content, source)
        compared_text = compact(_plain_text_node(comparison, excluded, parser=source.html_parser))
        if not (removed or em_removed or anchor_removed or factset_removed or stock_list_removed) or compared_text not in schema_views:
            comment_witness = _cnyes_schema_legacy_comment_witness(
                container, full_content, source, excluded, reveal, schema_views)
            if comment_witness is not None:
                compared_text, verified_schema_legacy_comment_references = comment_witness
            else:
                caption_view = _cnyes_caption_schema_view(container, full_content, source, excluded, reveal)
                if caption_view is None:
                    raise _cnyes_native_body_failure('schema_mismatch_without_verified_figure_view')
                (comparison, verified_captions, verified_notices,
                 verified_empty_caption_placeholders, verified_empty_figure_placeholders) = caption_view[:5]
                removed = _cnyes_remove_leading_paragraph_markers(comparison)
                em_removed = _cnyes_remove_verified_em_paragraph_markers(comparison, full_content, source)
                anchor_removed = _cnyes_remove_verified_anchor_markers(comparison, full_content, source)
                factset_removed = _cnyes_remove_verified_factset_markers(comparison, full_content, source)
                stock_list_removed = _cnyes_remove_verified_stock_list_markers(comparison, full_content, source)
                compared_text = compact(_plain_text_node(comparison, excluded, parser=source.html_parser))
                if compared_text not in schema_views:
                    raise _cnyes_native_body_failure('schema_comparison_mismatch')
                if caption_view.narrative_body is not None:
                    visible = caption_view.narrative_body
                    verified_caption_wrapped_narrative = 'same_article_caption_gallery_narrative_tail' in caption_view.narrative_kinds
                    verified_image_leaf_prose_narrative = 'same_article_image_leaf_prose_figure' in caption_view.narrative_kinds
                    verified_image_paragraph_narrative = 'same_article_image_paragraph_narrative' in caption_view.narrative_kinds
                    verified_ordered_image_leaf_paragraphs = 'same_article_ordered_image_leaf_paragraphs' in caption_view.narrative_kinds
                    verified_matched_figure_paragraphs = 'same_article_matched_figure_paragraphs' in caption_view.narrative_kinds
        schema_compared_paragraph_markers = bool(removed)
        schema_compared_em_paragraph_markers = bool(em_removed)
        schema_compared_anchor_markers = bool(anchor_removed)
        schema_compared_factset_markers = bool(factset_removed)
        schema_compared_stock_list_markers = bool(stock_list_removed)
    verified_notices = verified_legacy_template_references + verified_schema_legacy_comment_references + verified_notices
    verified_nested_tables = False
    if container.find('table'):
        from bs4 import BeautifulSoup
        declared_content = BeautifulSoup(html.unescape(full_content), source.html_parser)
        grids = _cnyes_table_grids(container, source)
        if grids is None:
            witness = _cnyes_nested_table_witness(container, source)
            if witness is None or witness != _cnyes_nested_table_witness(declared_content, source):
                raise _cnyes_native_body_failure('table_grid_mismatch')
            verified_nested_tables = True
        elif not grids or grids != _cnyes_table_grids(declared_content, source):
            raise _cnyes_native_body_failure('table_grid_mismatch')
    for node in container.find_all(['p', 'td', 'th', 'li', 'a']):
        if not node.find_parent(['figure', 'figcaption', 'nav', 'aside']):
            _cnyes_visible_after_completed_stream(node, reveal)
    return {**article, 'articleBody': html.escape(visible),
            'attribution_status': ('source_declared_author_visible_signature_empty'
                                   if not identity.visible_byline else 'source_byline_present'),
            'article_identity_basis': ('actual_same_id_mobile_canonical_og_schema_visible_body_and_current_free_data_v1'
                if parts.hostname == 'm.cnyes.com' else 'actual_same_id_desktop_query_canonical_og_schema_visible_body_and_current_free_data_v1')
                + ('; source_visible_title_outer_whitespace_comparison_v1' if visible_title != data['title'] else '')
                + ('; source_current_named_author_and_explicit_empty_visible_signature_v1'
                   if not identity.visible_byline else '')
                + ('; verified_completed_fizz_boundary' if reveal is not None else '')
                + ('; source_paragraph_format_marker_comparison_v1' if schema_compared_paragraph_markers else '')
                + ('; source_current_em_paragraph_format_marker_comparison_v1' if schema_compared_em_paragraph_markers else '')
                + ('; source_current_nested_table_cell_forest_comparison_v1' if verified_nested_tables else '')
                + ('; source_anchor_title_format_marker_comparison_v1' if schema_compared_anchor_markers else '')
                + ('; source_factset_company_prefix_marker_comparison_v1' if schema_compared_factset_markers else '')
                + ('; source_stock_list_company_prefix_marker_comparison_v1' if schema_compared_stock_list_markers else '')
                + ('; source_current_figure_caption_schema_comparison_v1' if verified_captions else '')
                + ('; source_current_figure_notice_schema_comparison_v1'
                   if any(note['kind'] not in {'same_article_legacy_template_reference', 'same_article_schema_legacy_comment_reference'}
                          for note in verified_notices) else '')
                + ('; source_current_empty_caption_placeholder_comparison_v1' if verified_empty_caption_placeholders else '')
                + ('; source_current_empty_figure_placeholder_comparison_v1' if verified_empty_figure_placeholders else '')
                + ('; source_current_empty_chart_caption_reference_comparison_v1'
                   if any(note['kind'] == 'same_article_empty_chart_caption_reference' for note in verified_notices) else '')
                + ('; source_current_typed_chart_caption_reference_comparison_v1'
                   if any(note['kind'] == 'same_article_typed_chart_caption_reference' for note in verified_notices) else '')
                + ('; source_current_nested_figure_schema_comparison_v1'
                   if any(note['kind'] == 'same_article_nested_figure_text' for note in verified_notices)
                   or any(caption.get('structure_basis') and caption['structure_basis'] not in {
                          'same_article_typed_chart_label_and_source_image',
                          'same_article_visible_labelled_image_paragraph_and_source_image'} for caption in verified_captions) else '')
                + ('; source_current_nested_mixed_figure_schema_comparison_v1'
                   if any(note['kind'] == 'same_article_nested_mixed_figure_text' for note in verified_notices) else '')
                + ('; source_current_caption_gallery_text_body_comparison_v1'
                   if verified_caption_gallery_text_body else '')
                + ('; source_current_caption_wrapped_narrative_body_comparison_v1'
                   if verified_caption_wrapped_narrative else '')
                + ('; source_current_image_leaf_prose_body_comparison_v1'
                   if verified_image_leaf_prose_narrative else '')
                + ('; source_current_ordered_figure_forest_comparison_v1'
                   if any(note['kind'] in _CNYES_ORDERED_FIGURE_KINDS for note in verified_notices) else '')
                + ('; source_current_ordered_image_leaf_paragraphs_body_comparison_v1'
                   if verified_ordered_image_leaf_paragraphs else '')
                + ('; source_current_matched_figure_paragraphs_body_comparison_v1'
                   if verified_matched_figure_paragraphs else '')
                + ('; source_current_image_paragraph_narrative_body_comparison_v1'
                   if verified_image_paragraph_narrative else '')
                + ('; source_current_labelled_image_paragraph_caption_comparison_v1'
                   if any(note['kind'] == 'same_article_labelled_image_paragraph_caption' for note in verified_notices) else '')
                + ('; source_current_linked_caption_figure_comparison_v1'
                   if any(note['kind'] == 'same_article_linked_caption_figure' for note in verified_notices) else '')
                + ('; source_current_warning_placeholder_figure_comparison_v1'
                   if any(note['kind'] == 'same_article_warning_placeholder_figure' for note in verified_notices) else '')
                + ('; source_current_narrative_figure_wrapper_comparison_v1'
                   if verified_figure_narrative_body else '')
                + ('; source_current_linked_image_figure_schema_comparison_v1'
                   if any(note['kind'] in {'same_article_image_figure_link_text', 'same_article_linked_image_reference'}
                          for note in verified_notices) else '')
                + ('; source_current_image_figure_supplement_schema_comparison_v1'
                   if any(note['kind'] == 'same_article_image_figure_text' for note in verified_notices) else '')
                + ('; source_current_image_figure_numbered_heading_comparison_v1'
                   if any(note['kind'] == 'same_article_image_figure_numbered_heading' for note in verified_notices) else '')
                + ('; source_current_image_figure_source_credit_comparison_v1'
                   if any(note['kind'] == 'same_article_image_figure_source_credit' for note in verified_notices) else '')
                + ('; source_current_image_figure_one_line_warning_comparison_v1'
                   if any(note['kind'] == 'same_article_image_figure_one_line_warning_supplement' for note in verified_notices) else '')
                + ('; source_current_image_figure_social_links_comparison_v1'
                   if any(note['kind'] == 'same_article_image_figure_social_links' for note in verified_notices) else '')
                + ('; source_current_image_figure_social_placeholder_comparison_v1'
                   if any(note['kind'] == 'same_article_image_figure_social_placeholder_links' for note in verified_notices) else '')
                + ('; source_current_caption_leaf_placeholder_comparison_v1'
                   if any(note['kind'] == 'same_article_caption_leaf_placeholder' for note in verified_notices) else '')
                + ('; source_current_figure_source_byline_comparison_v1'
                   if any(note['kind'] == 'same_article_figure_source_byline' for note in verified_notices) else '')
                + ('; source_current_legacy_template_reference_comparison_v1' if verified_legacy_template_references else '')
                + ('; source_current_schema_legacy_comment_reference_comparison_v1'
                   if verified_schema_legacy_comment_references else '')
                + ('; source_declared_article_body_literal_text_comparison_v1'
                   if compared_text == literal_schema and compared_text != parsed_schema else '')
                + (f'; observed_{parts.query.replace("=", "_")}_request_variant_with_full_current_article_proof_v1' if parts.query else ''),
            'verified_image_captions_json': json.dumps(verified_captions, ensure_ascii=False) if verified_captions else None,
            'verified_article_notices_json': json.dumps(verified_notices, ensure_ascii=False) if verified_notices else None,
            'published_time_basis': 'matched_current_visible_datetime_and_declared_clock; source_seconds_and_offset_preserved',
            'body_completeness': ('verified_original_visible_text; current_and_renderer_comments_match_schema_reference_order; '
                                  'schema_only_references_preserved_without_hidden_text_asset_or_value_reconstruction'
                                  if verified_schema_legacy_comment_references else
                                  'verified_original_visible_text; prose_and_ordered_current_legacy_comment_references_verified; '
                                  'original_references_preserved_without_asset_or_value_reconstruction'
                                  if verified_legacy_template_references else
                                  'verified_current_article_container; prose_matches_current_free_declarations; '
                                  'source_figure_byline_preserved_without_author_reassignment'
                                  if any(note['kind'] == 'same_article_figure_source_byline' for note in verified_notices) else
                                  'verified_current_visible_matched_figure_paragraphs_body; '
                                  'all_source_paragraphs_values_and_disclosures_preserved_in_original_order; '
                                  'explicit_figcaptions_and_image_link_DOM_references_preserved_separately'
                                  if verified_matched_figure_paragraphs else
                                  'verified_current_visible_ordered_image_leaf_paragraphs_body; '
                                  'all_source_paragraphs_values_and_disclosures_preserved_in_original_order; '
                                  'image_and_link_references_preserved_separately'
                                  if verified_ordered_image_leaf_paragraphs else
                                  'verified_current_visible_image_paragraph_narrative_body; '
                                  'all_source_paragraphs_values_and_disclosures_preserved_in_original_order; '
                                  'image_and_link_references_preserved_separately'
                                  if verified_image_paragraph_narrative else
                                  'verified_current_visible_image_leaf_prose_body; '
                                  'all_source_prose_disclosures_and_values_preserved_in_original_order; '
                                  'image_and_link_references_preserved_separately'
                                  if verified_image_leaf_prose_narrative else
                                  'verified_current_visible_caption_wrapped_narrative_body; '
                                  'all_source_prose_preserved_in_order; original_image_captions_preserved_separately'
                                  if verified_caption_wrapped_narrative else
                                  'verified_current_visible_narrative_figure_body; '
                                  'all_source_prose_and_fund_disclosures_preserved; image_leaves_excluded'
                                  if verified_figure_narrative_body else
                                  'verified_current_visible_caption_gallery_text; original_captions_in_source_order; '
                                  'image_pixels_not_downloaded_or_OCRed'
                                  if verified_caption_gallery_text_body else
                                  'verified_current_article_container; visible_narrative_matches_current_free_declarations; '
                                  'mixed_nested_figure_text_preserved_separately_in_verified_article_notices_json'
                                  if any(note['kind'] == 'same_article_nested_mixed_figure_text' for note in verified_notices) else
                                  'verified_current_article_container; visible_narrative_matches_current_free_declarations; '
                                  'image_figure_link_text_and_references_preserved_separately_in_verified_article_notices_json'
                                  if any(note['kind'] in {'same_article_image_figure_link_text', 'same_article_linked_image_reference'}
                                         for note in verified_notices) else
                                  'verified_current_article_container; visible_narrative_matches_current_free_declarations; '
                                  'one_line_warning_supplement_preserved_separately_in_verified_article_notices_json'
                                  if any(note['kind'] == 'same_article_image_figure_one_line_warning_supplement'
                                         for note in verified_notices) else
                                  'verified_current_article_container; visible_narrative_matches_current_free_declarations; '
                                  'social_link_supplement_preserved_separately_in_verified_article_notices_json'
                                  if any(note['kind'] in {'same_article_image_figure_social_links',
                                                         'same_article_image_figure_social_placeholder_links'}
                                         for note in verified_notices) else
                                  'verified_current_article_container; visible_body_matches_current_free_canonical_declarations')
                + ('; ordered_native_figure_labels_credits_notices_and_DOM_witnesses_preserved_separately'
                   if any(note['kind'] in _CNYES_ORDERED_FIGURE_KINDS for note in verified_notices) else ''),
            'author_attribution_detail_json': json.dumps({
                'visible_byline_raw': identity.visible_byline,
                'schema_authors_raw': identity.author_names, 'current_data_author_raw': data['author'],
                'declared_canonical_url': identity.declared_url,
                'visible_byline_explicitly_empty': not bool(identity.visible_byline),
                'named_author_invented': False,
                'byline_basis': ('matched_current_schema_and_current_data_named_author; explicit_empty_visible_signature_preserved'
                                 if not identity.visible_byline else
                                 'matched_current_declared_author; visible_credit_preserved_separately')},
                ensure_ascii=False, sort_keys=True)}


# Existing callers retain one implementation, never another acquisition path.
cnyes_mobile_canonical_article = cnyes_native_canonical_article


def cnyes_verified_image_only_body(soup, source, article, *, page_url, identity=None):
    """Prove observed same-article image-only forests, not missing narrative.

    Preserve free/native identity, aware clocks and current/schema authors;
    an explicitly observed desk credit is not a visible named reporter. The
    bounded forest matches source image order, attributes and whole structure.
    Alt text, OCR, remote media and unknown mixed content never supply a body.
    """
    from downloader.news_fulltext_policy import normalize_url, _plain_text_node
    from downloader.news_fulltext_cnyes_images import same_image_only_view

    if source.provider != 'cnyes' or article.get('@type') != 'NewsArticle' or article.get('articleBody', '') != '':
        return False
    try:
        expected = normalize_url(page_url)
        parts = urlsplit(expected)
        match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
        if (not match or parts.scheme != 'https' or parts.fragment or parts.hostname not in {'news.cnyes.com', 'm.cnyes.com'}
                or parts.query and not cnyes_supported_native_alias_view(parts)):
            return False
        canonical = soup.find_all('link', rel='canonical')
        scope = _cnyes_visible_article_scope(soup)
        if len(canonical) != 1 or scope is None:
            return False
        declared = normalize_url(canonical[0].get('href', ''))
        expected_declared = identity.declared_url if identity else expected
        main = article.get('mainEntityOfPage')
        main = main.get('@id') if isinstance(main, dict) else main
        if (declared != expected_declared or normalize_url(article.get('url', '')) != declared
                or normalize_url(main) != declared or str(article.get('isAccessibleForFree', 'true')).lower() == 'false'):
            return False
        header, container = scope
        if container.find('img') is None:
            return False  # Ordinary empty templates need no Flight/current reparse.
        parent = container.find_parent('article')
        reveal = identity.revealed_wrapper if identity else _cnyes_completed_stream_wrapper(soup, parent)
        data = _cnyes_current_article_data(soup, int(match[1]), verify_video=True, verify_author=True)
        clock = datetime.fromisoformat(article.get('datePublished', '').replace('Z', '+00:00'))
        times = parent.select(':scope > p > time[datetime]')
        bylines = parent.select(':scope > p > .signature')
        authors = article.get('author')
        authors = authors if isinstance(authors, list) else [authors]
        names = [node.get('name') for node in authors if isinstance(node, dict)]
        if (data is None or type(data.get('payment')) is not int or data['payment'] != 0
                or type(data.get('publishAt')) is not int or clock.utcoffset() is None or data['publishAt'] != clock.timestamp()
                or data.get('url') != declared or data.get('title') != article.get('headline')
                or not cnyes_visible_headline_matches(header.get_text(' ', strip=True), data['title'])
                or len(times) != 1 or times[0]['datetime'] != article['datePublished']
                or len(names) != 1 or not isinstance(names[0], str) or not names[0].strip()
                or names[0] != data.get('author') or len(bylines) != 1 or not bylines[0].get_text(strip=True)
                or (names[0] not in bylines[0].get_text(' ', strip=True)
                    and bylines[0].get_text(' ', strip=True) not in {'鉅亨網新聞中心', '鉅亨台北資料中心'}) or data.get('video', '') != ''
                or not isinstance(data.get('content'), str)):
            return False
        for node in (header, container, times[0], bylines[0]):
            _cnyes_visible_after_completed_stream(node, reveal)
        excluded = (*source.exclude_selectors, '[id^="div-gpt-ad-cnyes_news_article_"]')
        if _plain_text_node(container, excluded, parser=source.html_parser):
            return False
        return same_image_only_view(container, data['content'], source, match[1],
            lambda node: _cnyes_visible_after_completed_stream(node, reveal))
    except (NewsError, KeyError, ValueError, TypeError, AttributeError, OverflowError):
        return False


def cnyes_verified_empty_body(soup, source, article, *, page_url):
    """Distinguish observed empty public templates from extraction failures.

    This is failure-outcome contract v7, not a new accepted-body contract.
    A metadata shell, hidden body, video or guessed URL is never a full article.
    Require matching public identity and an aware declared publication clock,
    an actually empty visible container and the same current free declaration.
    """
    from downloader.news_fulltext_policy import normalize_url

    if source.provider != 'cnyes' or article.get('@type') != 'NewsArticle':
        return False
    try:
        expected = normalize_url(page_url)
        parts = urlsplit(expected)
        match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
        canonical = soup.find_all('link', rel='canonical')
        headline = article.get('headline')
        visible_scope = _cnyes_visible_article_scope(soup)
        if visible_scope is None:
            return False
        header, container = visible_scope
        clock = datetime.fromisoformat(article.get('datePublished', '').replace('Z', '+00:00'))
        declared_body = article.get('articleBody', '')
        visible_body = container.get_text(' ', strip=True)
        # Native company-announcement pages can render the literal string
        # "null" in JSON-LD, the visible <p>, and the current free API record.
        # That is a source placeholder, not four characters of article text.
        # None, whitespace, case variants or mixed real text stay parser gaps.
        if declared_body not in ('', 'null') or visible_body != declared_body:
            return False
        if (parts.hostname not in {'news.cnyes.com', 'm.cnyes.com'} or not match or len(canonical) != 1
                or normalize_url(canonical[0].get('href', '')) != expected
                or normalize_url(article.get('url', '')) != expected
                or clock.tzinfo is None or not isinstance(headline, str)
                or re.sub(r'\s+', '', header.get_text(' ', strip=True)) != re.sub(r'\s+', '', headline)
                or container.find(['img', 'video', 'audio', 'iframe', 'object', 'embed'])):
            return False
        data = _cnyes_current_article_data(soup, int(match[1]), verify_video=True)
        content = data.get('content') if data else None
        # Response 784083 declares an encoded, exactly empty div; 958337 has
        # one BR inside that div, with no schema/visible text. Do not generalize
        # these exact source declarations to arbitrary textless HTML:
        # scripts, hidden text, comments, attributes and media stay unresolved.
        content_matches = (isinstance(content, str) and html.unescape(content) in (
                           '', '<div></div>', '<div><br></div>', '<p></p>',
                           '<p>&nbsp;</p>\n<div>\n<p>&nbsp;</p>\n</div>',
                           '<p>\xa0</p>\n<div>\n<p>\xa0</p>\n</div>')
                           if declared_body == '' else
                           isinstance(content, str) and html.unescape(content) in ('null', '<p>null</p>'))
        return bool(data and type(data.get('payment')) is int and data['payment'] == 0
                    and content_matches and data.get('video', '') == ''
                    and data.get('title') == headline and normalize_url(data.get('url', '')) == expected
                    and type(data.get('publishAt')) is int and data['publishAt'] == clock.timestamp())
    except (NewsError, ValueError, TypeError, AttributeError, OverflowError):
        return False


def _globalvoices_visible_article_identity(soup, source, *, page_url):
    """Bind exceptional short/nontext outcomes to one declared article."""
    from downloader.news_fulltext_policy import normalize_url
    from downloader.news_fulltext_public_originals import _visible

    if source.provider != 'globalvoices' or not page_url:
        return None
    try:
        expected = normalize_url(page_url)
        parts = urlsplit(expected)
        if (parts.scheme != 'https' or not parts.hostname
                or not (parts.hostname == 'globalvoices.org' or parts.hostname.endswith('.globalvoices.org'))):
            return None
        canonical = soup.select('link[rel="canonical"]')
        containers = soup.select('.post-container')
        heads = soup.select(source.title_selector) if source.title_selector else []
        credits = soup.select('#post-header-credit')
        clocks = soup.select('meta[property="article:published_time"]')
        if (len(canonical) != 1 or normalize_url(canonical[0].get('href', '')) != expected
                or len(containers) != 1 or len(heads) != 1 or len(credits) != 1 or len(clocks) != 1):
            return None
        entries = containers[0].select('.entry')
        if len(entries) != 1:
            return None
        for node in (containers[0], entries[0], heads[0], credits[0]):
            _visible(node)
        headline = heads[0].get_text(' ', strip=True)
        published = clocks[0].get('content', '')
        clock = datetime.fromisoformat(published.replace('Z', '+00:00'))
        if not headline or clock.tzinfo is None:
            return None

        def named_profiles(nodes):
            result = []
            for node in nodes:
                name = node.get_text(' ', strip=True)
                if not name:
                    continue  # The source also renders empty avatar links.
                _visible(node)
                profile = normalize_url(urljoin(expected, node.get('href', '')))
                parts = urlsplit(profile)
                if (parts.scheme != 'https' or not parts.hostname
                        or not (parts.hostname == 'globalvoices.org' or parts.hostname.endswith('.globalvoices.org'))
                        or not re.fullmatch(r'/author/[^/]+/?', parts.path)):
                    return None
                result.append((name, profile))
            return list(dict.fromkeys(result))

        writers = named_profiles(credits[0].select('a.user-link'))
        footer_writers = named_profiles(containers[0].select('.postfooter-credits .contributor-name a.user-link'))
        licenses = containers[0].select('.post-credit-container [rel="license"]')
        for node in licenses:
            _visible(node)
        if (not writers or writers != footer_writers or not source.required_license_url_prefix
                or not any(str(n.get('href', '')).startswith(source.required_license_url_prefix) for n in licenses)):
            return None
        return entries[0], {'headline': headline, 'author': [{'name': name} for name, _ in writers],
                            'datePublished': published}
    except NewsError:
        return None
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def globalvoices_verified_small_body(soup, source, body, *, page_url):
    """Verify complete micro-posts; never accept media/summary as their text."""
    from downloader.news_fulltext_policy import _plain_text_node
    from downloader.news_fulltext_public_originals import _visible

    if not 0 < len(body) < 100:
        return None
    identity = _globalvoices_visible_article_identity(soup, source, page_url=page_url)
    if identity is None:
        return None
    entry, article = identity
    media = entry.find_all(['video', 'audio', 'iframe', 'object', 'embed', 'script'])
    static_video = len(media) == 1 and _globalvoices_verified_static_video(media[0])
    if (media and not static_video
            or entry.select('.contributors')
            or _globalvoices_context_text(body) == _GLOBALVOICES_PREVIOUS_REPORT_TEXT
            or any(node.name == 'p' and _globalvoices_context_text(node.get_text(' ', strip=True))
                   == _GLOBALVOICES_PREVIOUS_REPORT_TEXT for node in entry.find_all(recursive=False))
            or _plain_text_node(entry, source.exclude_selectors, parser=source.html_parser) != body):
        return None
    images = entry.find_all('img')
    if images and static_video:
        return None  # No mixed-media short layout has been verified.
    try:
        for image in images:
            # Source-authored text outside static caption containers is still
            # text. Do not accept an unmarked image/unknown loading structure
            # as proof of a complete narrative, or extract third-party captions.
            caption = image.find_parent(class_='wp-caption')
            if caption is None or caption.find_parent(class_='entry') is not entry:
                return None
            _visible(image)
        checked = set()
        for text in entry.find_all(string=True):
            parent = text.parent
            if (not str(text).strip() or id(parent) in checked
                    or parent.find_parent(class_='wp-caption') is not None
                    or 'wp-caption-text' in parent.get('class', [])):
                continue
            _visible(parent)
            checked.add(id(parent))
    except NewsError:
        return None
    if body == '[Pictochart.com embed removed because it no longer worked]':
        raise NewsError('source_body_removed')
    return {**article,
            'article_identity_basis': 'canonical_visible_micro_post; matching_header_footer_authors_clock_and_article_license',
            'body_completeness': ('verified_publisher_short_body; one_visible_article_container_not_summary'
                                  + ('; excluded_source_caption_containers; embedded_images_not_downloaded_or_OCRed'
                                     if images else '')
                                  + ('; external_static_video_not_requested_or_transcribed' if static_video else ''))}


_GLOBALVOICES_PREVIOUS_REPORT_TEXT = "More information see GV's previous report."


def _globalvoices_context_text(text):
    # Renderer/newline differences must not turn navigation into a narrative.
    return re.sub(r'\s+\.', '.', re.sub(r'\s+', ' ', text).strip())


def _globalvoices_verified_static_video(node):
    """Review one native embed declaration, never access its third-party data."""
    from downloader.news_fulltext_public_originals import _visible

    if node.name != 'iframe' or node.find_all() or node.has_attr('srcdoc'):
        return False
    try:
        _visible(node)
        parts = urlsplit(str(node.get('src', '')))
        return (parts.scheme == 'https' and parts.netloc == 'player.vimeo.com'
                and re.fullmatch(r'/video/[1-9][0-9]{0,15}', parts.path) is not None
                and parts.query == 'dnt=1&app_id=122963' and not parts.fragment)
    except (NewsError, ValueError):
        return False


def _globalvoices_verified_nontext_youtube(node):
    """Exact observed native declaration; no media fetch or text exemption."""
    from downloader.news_fulltext_public_originals import _visible

    if node.name != 'iframe' or node.find_all() or node.has_attr('srcdoc'):
        return False
    try:
        _visible(node)
        parts = urlsplit(str(node.get('src', '')))
        return (parts.scheme == 'https' and parts.netloc == 'www.youtube.com'
                and re.fullmatch(r'/embed/[A-Za-z0-9_-]{11}', parts.path) is not None
                and parts.query == 'feature=oembed' and not parts.fragment)
    except (NewsError, ValueError):
        return False


def _globalvoices_verified_image_context(node, *, page_url):
    """Recognize native navigation/credit fields, not arbitrary short text.

    These declarations occur beside old photo posts. They are retained in the
    response, but cannot supply narrative text. No linked report is fetched.
    Unknown markup, credit labels, hidden fields and foreign links stay gaps.
    """
    from downloader.news_fulltext_policy import normalize_url
    from downloader.news_fulltext_public_originals import _visible

    try:
        _visible(node)
        descendants = node.find_all()
        if node.name == 'div' and node.get('class') == ['contributors']:
            lines = node.get_text('\n', strip=True).splitlines()
            name = r"[A-Za-z\u3400-\u9fff][A-Za-z0-9\u3400-\u9fff .'-]{0,63}"
            if not descendants and len(lines) == 1:
                # Native one-field layout: proofreader credit beside a photo,
                # not a missing article or an inferred source author.
                return re.fullmatch(r'校對[：:][ \t]*' + name, lines[0]) is not None
            if len(descendants) != 1 or descendants[0].name != 'br':
                return False
            return (len(lines) == 2
                    and re.fullmatch(r'譯者[：:][ \t]*' + name, lines[0]) is not None
                    and re.fullmatch(r'校對[：:][ \t]*' + name, lines[1]) is not None)
        if (node.name != 'p' or len(descendants) != 1 or descendants[0].name != 'a'
                or _globalvoices_context_text(node.get_text(' ', strip=True)) != _GLOBALVOICES_PREVIOUS_REPORT_TEXT):
            return False
        link = descendants[0]
        _visible(link)
        href = normalize_url(urljoin(page_url, link.get('href', '')))
        target = urlsplit(href)
        return (href != normalize_url(page_url) and target.scheme == 'https'
                and target.hostname in {'globalvoices.org', 'globalvoicesonline.org'}
                and not target.query and not target.fragment
                and re.fullmatch(r'/[0-9]{4}/[0-9]{2}/[0-9]{2}/[a-z0-9-]+/', target.path) is not None)
    except (NewsError, ValueError, TypeError, AttributeError):
        return False


def globalvoices_verified_nontext_body(soup, source, *, page_url):
    """Classify reviewed native nontext pages, not missing text as success.

    Excluded third-party image captions stay excluded. Known external widget
    declarations prove only an embedded representation, not its accessibility,
    contents, copyright or historical completeness. No media/script requests.
    Unknown empty/loading structures remain parser gaps.
    """
    from copy import deepcopy
    from downloader.news_fulltext_policy import _plain_text_node
    from downloader.news_fulltext_public_originals import _visible

    identity = _globalvoices_visible_article_identity(soup, source, page_url=page_url)
    if identity is None:
        return None
    entry, _ = identity
    scripts = entry.find_all('script')
    media = entry.find_all(['video', 'audio', 'iframe', 'object', 'embed'])
    images = entry.find_all('img')
    remaining = _plain_text_node(entry, source.exclude_selectors, parser=source.html_parser)
    if remaining:
        if not images or media or scripts:
            return None
        # Only the absence-classification path copies the bounded entry. Do
        # not mutate the shared soup, source bytes or normal article hot path.
        reviewed = deepcopy(entry)
        for node in reviewed.find_all(recursive=False):
            if _globalvoices_verified_image_context(node, page_url=page_url):
                node.decompose()
        if _plain_text_node(reviewed, source.exclude_selectors, parser=source.html_parser):
            return None
    if not scripts and len(media) == 1 and _globalvoices_verified_nontext_youtube(media[0]):
        # The real 2012 documentary page has an excluded third-party photo
        # caption and one visible static embed, with no native narrative.
        # Unmarked/hidden photos and unknown widgets remain parser gaps.
        try:
            for image in images:
                caption = image.find_parent(class_='wp-caption')
                if caption is None or caption.find_parent(class_='entry') is not entry:
                    return None
                _visible(image)
        except NewsError:
            return None
        return 'source_body_embedded_only'
    if images and not media and not scripts:
        try:
            for node in images:
                _visible(node)
        except NewsError:
            return None
        return 'source_body_image_only'
    if not images and not scripts and len(media) == 1 and media[0].name == 'iframe':
        if _globalvoices_verified_static_video(media[0]):
            return 'source_body_embedded_only'
        try:
            src = urlsplit(str(media[0].get('src', '')))
        except ValueError:
            return None
        if (src.scheme == 'https' and src.netloc == 'magic.piktochart.com'
                and not src.query and not src.fragment
                and re.fullmatch(r'/embed/[0-9]+-[a-z0-9-]+', src.path)):
            return 'source_body_embedded_only'
    widgets = entry.select('.pb_feed[data-game][data-embed-by]')
    if not images and not media and len(scripts) == 1 and len(widgets) == 1:
        widget = widgets[0]
        if (urljoin(page_url, str(scripts[0].get('src', ''))) == 'https://cdn.playbuzz.com/widget/feed.js'
                and re.fullmatch(r'/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+', str(widget.get('data-game', '')))
                and re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', str(widget.get('data-embed-by', '')))):
            return 'source_body_embedded_only'
    return None


def cnyes_verified_short_body(soup, source, article, body, *, page_url, restore_source_spacing=False):
    """Confirm a short, already-declared public body; never supply new text.

    Actual Cnyes flash reports have 35--78 characters. A length threshold is
    not completeness evidence. Require the canonical identity, JSON-LD clock,
    whole visible container/tables and current free/full-content declaration to
    agree. The returned body is visible text, never referenced hidden text.
    Hydration corroborates the visible text; it cannot provide a hidden
    body, related story, summary, inferred date or paid content.
    """
    from downloader.news_fulltext_policy import _plain_text, _plain_text_node, normalize_url, _NON_BODY_TAGS

    if source.provider != 'cnyes' or not 0 < len(body) < 100 or article.get('@type') != 'NewsArticle':
        return None
    try:
        expected = normalize_url(page_url)
        parts = urlsplit(expected)
        match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
        canonical = soup.find_all('link', rel='canonical')
        if (parts.hostname not in {'news.cnyes.com', 'm.cnyes.com'} or not match or len(canonical) != 1
                or normalize_url(canonical[0].get('href', '')) != expected
                or normalize_url(article.get('url', '')) != expected):
            return None
        declared_clock = datetime.fromisoformat(article.get('datePublished', '').replace('Z', '+00:00'))
        if declared_clock.tzinfo is None:
            return None
    except (NewsError, ValueError, TypeError, AttributeError):
        return None

    compact = lambda text: re.sub(r'\s+', '', text)
    headline = article.get('headline')
    visible_scope = _cnyes_visible_article_scope(soup)
    if visible_scope is None:
        return None
    header, container = visible_scope
    if (not isinstance(headline, str) or not headline.strip()
            or compact(header.get_text(' ', strip=True)) != compact(headline)):
        return None
    # Figure captions, ads, navigation and related-story text do not count.
    # A paragraph-only check loses valid short revenue tables. Keep cell
    # boundaries in the output instead of concatenating 22 and 184 to 22184.
    # 5154231's complete 99-character flash includes direct text/BR outside
    # paragraphs. Selecting only p/table drops real observations. Whole visible
    # text and full current content remain mandatory, not a teaser exception.
    excluded = (*source.exclude_selectors, '[id^="div-gpt-ad-cnyes_news_article_"]')
    public_text = _plain_text_node(container, excluded, parser=source.html_parser)
    if compact(public_text) != compact(body):
        return None
    try:
        reveal = _cnyes_completed_stream_wrapper(soup, container.find_parent('article'))
        _cnyes_visible_after_completed_stream(header, reveal)
        excluded_roots = {id(node) for selector in excluded for node in container.select(selector)}
        blocked = set()
        for node in (container, *container.find_all(True)):
            if id(node.parent) in blocked or id(node) in excluded_roots or node.name in _NON_BODY_TAGS:
                blocked.add(id(node))
            else:
                _cnyes_visible_after_completed_stream(node, reveal)
    except NewsError:
        return None
    data = _cnyes_current_article_data(soup, int(match[1]))
    if data is None:
        return None
    try:
        if (normalize_url(data.get('url', '')) != expected or not isinstance(data.get('title'), str)
                or compact(data['title']) != compact(headline) or type(data.get('publishAt')) is not int
                or declared_clock.timestamp() != data['publishAt'] or type(data.get('payment')) is not int
                or data['payment'] != 0 or not isinstance(data.get('content'), str)):
            return None
    except (NewsError, ValueError, TypeError, OverflowError):
        return None
    declared = _plain_text(html.unescape(data['content']), parser=source.html_parser)
    if not declared or compact(declared) != compact(body):
        return None
    tables = container.find_all('table')
    if not tables:
        if restore_source_spacing:
            # JSON-LD can concatenate English words even though the whole
            # visible/current article retains them. Restore only their actual
            # shared whitespace, never inferred words or numeric boundaries.
            normalize_spaces = lambda text: re.sub(r'\s+', ' ', text).strip()
            if (len(public_text.split()) < 8 or not re.search(r'[A-Za-z]', public_text)
                    or normalize_spaces(declared) != normalize_spaces(public_text)
                    or container.find(['img', 'video', 'audio', 'iframe', 'object', 'embed', 'figure'])):
                return None
            return public_text
        return body
    from bs4 import BeautifulSoup
    declaration = BeautifulSoup(html.unescape(data['content']), source.html_parser)

    visible_grids = _cnyes_table_grids(container, source)
    if (not visible_grids or visible_grids != _cnyes_table_grids(declaration, source)
            or container.find(['img', 'video', 'audio', 'iframe', 'object', 'embed'])):
        return None
    return public_text


def cnyes_verified_short_prose_spacing(soup, source, article, body, *, page_url):
    """Cheap candidate gate before full native identity/current-content proof.

    The existing eight-word guard stays in force on the actual source prose.
    Tiny/quote-only/unknown bodies do not parse the native stream again. This
    repairs a representation loss, not a missing publication or article body.
    """
    from downloader.news_fulltext_policy import _plain_text_node

    if (source.provider != 'cnyes' or not isinstance(body, str) or not 0 < len(body) < 100
            or re.search(r'[\u3400-\u9fff]', body)):
        return None
    scope = _cnyes_visible_article_scope(soup)
    if scope is None or scope[1].find(['img', 'video', 'audio', 'iframe', 'object', 'embed', 'table', 'figure']):
        return None
    visible = _plain_text_node(scope[1], (*source.exclude_selectors,
        '[id^="div-gpt-ad-cnyes_news_article_"]'), parser=source.html_parser)
    if len(visible.split()) < 8 or not re.search(r'[A-Za-z]', visible):
        return None
    return cnyes_verified_short_body(soup, source, article, body, page_url=page_url, restore_source_spacing=True)


def cnyes_verified_quote_only_body(soup, source, article, body, *, page_url):
    """A verified native symbol/price stub is not missing parser text or prose news.

    Retained 2013 responses agree in visible/current/schema representation but
    contain only ticker/price, ticker/price/change or price/change/company tokens.
    Actual 1693999 has only price/signed change/signed percent, without a ticker
    or colon. Its complete same-ID free current/visible/schema proof remains
    mandatory; a bare price or an extra sentence never qualifies. Classify that
    narrow source outcome, retaining raw evidence; never manufacture narrative.
    Original 1462347 contains only an alphabetic ticker token, without price.
    Its own source-video declaration must also be empty; this outcome does not
    assert a price, narrative or downloaded full-text article.
    Other retained originals add a colon to that token, put a ticker before
    the signed quote, or label the quote with a company/ticker after it. The
    source also retains an extra opening parenthesis in two symbol/price stubs.
    Classify their entire literal grammar without repairing source spelling,
    splitting unsigned numbers, or discarding words to force a match.
    """
    if source.provider != 'cnyes' or not isinstance(body, str) or not 0 < len(body) < 100:
        return False
    number = r'[0-9]+(?:\.[0-9]+)?'
    symbol_stub = r'\([A-Z][A-Z0-9.-]{0,15}\)'
    ticker = rf'\([A-Z][A-Z0-9.-]{{0,15}}\)\s*{number}(?:\s*[+-]{number})?\s*:'
    company_quote = rf'{number}\s*[+-]{number}\([+-]{number}%\)\s*[:：]\s*[A-Za-z][A-Za-z0-9 .&\x27-]{{0,50}}'
    bare_quote = rf'{number}\s*[+-]{number}\([+-]{number}%\)'
    labelled_quote = rf'{symbol_stub}\s*{bare_quote}'
    named_quote = rf'{bare_quote}\s*[:：]\s*[A-Za-z][A-Za-z0-9 .&\x27-]{{0,50}}\s*{symbol_stub}'
    percent_label = rf'{number}\s*[+-]{number}\s*[+-]{number}%\s*[:：]\s*{symbol_stub}'
    extra_opening_symbol = rf'\({symbol_stub}\s*{number}\s*:'
    symbol_only = rf'{symbol_stub}\s*:?'
    matches_quote = (re.fullmatch(symbol_stub, body.strip()) or re.fullmatch(ticker, body.strip())
                     or re.fullmatch(company_quote, body.strip()) or re.fullmatch(bare_quote, body.strip())
                     or re.fullmatch(labelled_quote, body.strip()) or re.fullmatch(named_quote, body.strip())
                     or re.fullmatch(percent_label, body.strip()) or re.fullmatch(extra_opening_symbol, body.strip())
                     or re.fullmatch(symbol_only, body.strip()))
    if not matches_quote:
        # Actual1673421's positive change and percent omit '+'. Its JSON-LD
        # concatenates adjacent numbers (20.191.08); that cannot prove their
        # boundaries. Require the independently verified visible/current text
        # with an actual separator, not an inferred numeric split or prose.
        from downloader.news_fulltext_policy import _plain_text_node
        scope = _cnyes_visible_article_scope(soup)
        if scope is None:
            return False
        visible = _plain_text_node(scope[1], (*source.exclude_selectors,
            '[id^="div-gpt-ad-cnyes_news_article_"]'), parser=source.html_parser)
        unsigned_ticker = rf'{symbol_stub}\s+{number}\s+{number}\({number}%\)\s*:'
        if re.fullmatch(unsigned_ticker, visible.strip()) is None:
            return False
    if cnyes_verified_short_body(soup, source, article, body, page_url=page_url) is None:
        return False
    if re.fullmatch(symbol_only, body.strip()):
        match = re.fullmatch(r'/news/id/([0-9]+)', urlsplit(page_url).path)
        data = _cnyes_current_article_data(soup, int(match[1]), verify_video=True) if match else None
        if not data or data.get('video', '') != '':
            return False
    scope = _cnyes_visible_article_scope(soup)
    return bool(scope and not scope[1].find(['img', 'video', 'audio', 'iframe', 'object', 'embed', 'table']))


def cmnews_visible_article(soup, *, page_url):
    """Match public SSR identity; hydration/related stories never supply text.

    The publisher's visible clock has minutes; a matching declared NewsArticle
    may preserve the actual seconds and offset. Without that matched metadata
    we retain the visible clock, never the URL, metadata sorting date or scrape
    time. All body extraction remains on the verified visible article scope.
    """
    from downloader.news_fulltext_policy import _objects, normalize_url

    try:
        expected = normalize_url(page_url)
        parts = urlsplit(expected)
    except (NewsError, ValueError):
        raise NewsError("article_identity_not_verified") from None
    match = re.fullmatch(r"/article/(.+)-([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})", parts.path, re.I)
    canonical = soup.find("link", rel="canonical")
    if parts.hostname != "cmnews.com.tw" or not match or canonical is None:
        raise NewsError("article_identity_not_verified")

    def same_url(value, target):
        try:
            return isinstance(value, str) and normalize_url(urljoin(expected, value)) == target
        except (NewsError, ValueError):
            return False

    if not same_url(canonical.get("href"), expected):
        raise NewsError("article_identity_not_verified")
    scopes = [n for n in soup.select(".articleContent[data-article-path]")
              if same_url(n.get("data-article-path"), expected)]
    if len(scopes) != 1:
        raise NewsError("article_identity_not_verified")
    scope = scopes[0]
    if (scope.get("data-author-id-article-id") != parts.path.rsplit("/", 1)[-1]
            or scope.get("id") != "ArticleContainer-" + match[2]):
        raise NewsError("article_identity_not_verified")
    heads = scope.select("h1.articleContent__title")
    bodies = scope.select(".articleContent__textContent")
    if len(heads) != 1 or not heads[0].get_text(" ", strip=True) or len(bodies) != 1:
        raise NewsError("article_identity_not_verified")
    # The live public SSR page can declare NewsArticle without a free/paid
    # flag, while its current body is masked for VIP login. Detect the exact
    # publisher mask inside this verified body, not navigation login buttons,
    # a quoted sentence or a neighboring infinite-scroll article. A longer
    # preview must never be promoted to a complete article either.
    for mask in bodies[0].select("vip-content.vipWrapper .vipContent .utilsMask"):
        titles = mask.select("p.utilsMask__title")
        descriptions = mask.select("p.utilsMask__desc")
        if (any(n.get_text(" ", strip=True) == "請登入以查看完整文章" for n in titles)
                or any(n.get_text(" ", strip=True) == "閱讀VIP文章請先登入理財寶會員" for n in descriptions)):
            raise NewsError("paywall_or_login")
    headline = heads[0].get_text(" ", strip=True)
    profile = parts.scheme + "://" + parts.netloc + "/" + match[1]
    bylines = scope.select(".articleContent__content--main > div:first-child > .flex.items-center > a[href]")
    if len(bylines) != 1 or not same_url(bylines[0].get("href"), profile):
        raise NewsError("article_attribution_not_verified")
    author = bylines[0].get_text(" ", strip=True)
    clocks = {n.get_text(" ", strip=True) for n in scope.select(".articleContent__time")} - {""}
    visible_clock = clocks.pop() if len(clocks) == 1 else None
    published_clock, modified_clock = visible_clock, None
    clock_parts = (re.fullmatch(r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2})'
                               r'(?: \(更新：([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2})\))?', visible_clock)
                   if isinstance(visible_clock, str) else None)
    if clock_parts:
        try:
            for value in clock_parts.groups():
                if value:
                    datetime.strptime(value, '%Y-%m-%d %H:%M')
            published_clock, modified_clock = clock_parts.groups()
        except ValueError:
            clock_parts = None
    result = {"headline": headline, "author": [{"name": author}], "datePublished": published_clock}
    if modified_clock:
        result.update(dateModified=modified_clock,
                      published_time_basis='publisher_visible_separate_published_and_updated_minute_clocks; source_precision_preserved')
    if not author:
        # Observed publisher SSR: a single empty <p> within the matching
        # current profile link, no NewsArticle schema, but a public visible
        # body and its own minute clock. A URL/profile slug is NOT a person's
        # name. Preserve that explicit source metadata gap rather than lose
        # the real body or borrow a footer, recommendation or hydration name.
        from downloader.news_fulltext_public_originals import _visible

        nodes = bylines[0].find_all(True)
        if (len(nodes) != 1 or nodes[0].name != 'p'
                or any(n.get('title') or n.get('aria-label') for n in (bylines[0], nodes[0]))
                or clock_parts is None):
            raise NewsError('article_attribution_not_verified')
        try:
            # The observed page declares only a BreadcrumbList, not a current
            # NewsArticle. Never treat navigation schema as a named byline,
            # and refuse other/ambiguous declarations for this narrow case.
            for script in soup.find_all('script', type='application/ld+json'):
                declaration = json.loads(script.get_text())
                if (not isinstance(declaration, dict) or declaration.get('@type') != 'BreadcrumbList'
                        or any(n.get('@type') not in {'BreadcrumbList', 'ListItem'} for n in _objects(declaration))):
                    raise ValueError('ambiguous source declaration')
            for node in (scope, heads[0], bodies[0], bylines[0], nodes[0], *scope.select('.articleContent__time')):
                _visible(node)
                if any(set(n.get('class', [])) & {'hidden', 'invisible', 'sr-only'}
                       for n in (node, *node.parents) if getattr(n, 'attrs', None)):
                    raise ValueError('hidden current source scope')
        except (NewsError, ValueError, TypeError):
            raise NewsError('article_attribution_not_verified') from None
        result.update(author=[], attribution_status='source_visible_byline_name_empty',
                      article_identity_basis='cmnews_canonical_current_uuid_profile_link_and_visible_empty_name_v1',
                      body_completeness='verified_current_public_ssr_article_container; visible_author_name_explicitly_empty',
                      published_time_basis='publisher_visible_minute_clock; no_seconds_or_timezone_inferred',
                      author_attribution_detail_json=json.dumps({
                          'visible_byline_raw': '', 'source_profile_url': profile,
                          'visible_clock_raw': visible_clock,
                          'declared_canonical_url': expected,
                          'byline_basis': 'matched_current_profile_link_with_explicit_empty_visible_name',
                          'named_author_invented': False}, ensure_ascii=False, sort_keys=True))
    matched = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            candidates = _objects(json.loads(script.get_text()))
            for candidate in candidates:
                types = candidate.get("@type", [])
                if isinstance(types, str):
                    types = [types]
                if (not isinstance(types, list) or "NewsArticle" not in types
                        or candidate.get("headline") != headline):
                    continue
                identity = candidate.get("mainEntityOfPage") or candidate.get("url") or candidate.get("@id")
                if isinstance(identity, dict):
                    identity = identity.get("@id") or identity.get("url")
                if identity and not same_url(identity, expected):
                    continue
                authors = candidate.get("author", [])
                if isinstance(authors, dict):
                    authors = [authors]
                if (not isinstance(authors, list) or len(authors) != 1 or not isinstance(authors[0], dict)
                        or authors[0].get("name") != author or not same_url(authors[0].get("url"), profile)):
                    continue
                if str(candidate.get("isAccessibleForFree", "true")).lower() == "false":
                    raise NewsError("paywall_or_login")
                clock = candidate.get("datePublished")
                if not isinstance(clock, str) or not published_clock:
                    continue
                try:
                    declared = datetime.fromisoformat(clock.replace("Z", "+00:00"))
                except ValueError:
                    raise NewsError("invalid_bulk_publication_clock") from None
                if declared.strftime("%Y-%m-%d %H:%M") != published_clock:
                    continue
                modified = candidate.get("dateModified")
                if modified_clock and isinstance(modified, str):
                    try:
                        updated = datetime.fromisoformat(modified.replace('Z', '+00:00'))
                    except ValueError:
                        raise NewsError('invalid_bulk_publication_clock') from None
                    if updated.strftime('%Y-%m-%d %H:%M') != modified_clock:
                        continue
                matched.append((clock, modified if isinstance(modified, str) else modified_clock))
        except (json.JSONDecodeError, TypeError):
            continue
    if len(set(matched)) == 1:
        result["datePublished"], result["dateModified"] = matched[0]
    # CMNews's editor wraps data tables in <figure class="table">. The
    # generic cleaner intentionally excludes image/byline figures, but that
    # default deleted the entire public announcement table. Unwrap only an
    # actual direct table in this verified article after access checks; retain
    # the original response bytes and keep every other figure excluded.
    for figure in bodies[0].find_all("figure", class_="table"):
        if len(figure.find_all("table", recursive=False)) == 1:
            figure.unwrap()
    return result, scope


def moneyweekly_article_identity(soup, *, page_url):
    """Verified source routes must resolve to the same declared article ID.

    Case/Chinese namespace aliases are not permission to strip the original
    query or substitute a guessed permalink. Preserve the request provenance.
    """
    def article_id(url):
        if not isinstance(url, str):
            return None
        try:
            parts = urlsplit(url)
            if (parts.hostname not in {'www.moneyweekly.com.tw', 'moneyweekly.com.tw'}
                    or parts.scheme not in {'http', 'https'} or parts.username or parts.password
                    or parts.port not in {None, 80 if parts.scheme == 'http' else 443}
                    or re.search(r'%(?:2f|5c)', parts.path, re.I)):
                return None
        except ValueError:
            return None
        match = re.fullmatch(r'/(?:ArticleData/Info|Home/ChangePage)/(?:(?i:article)|理財周刊)/([1-9][0-9]*)/?', unquote(parts.path))
        return match[1] if match else None

    canonical = soup.find('link', rel='canonical')
    requested_id = article_id(page_url)
    if not requested_id or canonical is None or article_id(canonical.get('href')) != requested_id:
        raise NewsError('article_identity_not_verified')
    headline = soup.find('h1')
    container = soup.select_one('.div_Article_Info[itemprop=articleBody]')
    # The public member-only response keeps a valid headline/canonical/byline,
    # but the verified current body contains only this publisher mask. Check
    # it before JSON-LD/body fallback; neither a preview nor hidden schema text
    # may turn the login notice into a complete article. Navigation login links
    # and an article quoting the wording are not this direct source mask.
    if container is not None:
        for mask in container.find_all('div', class_='mt-5', recursive=False):
            if re.fullmatch(r'》本文限會員登入瀏覽，請先登入(?:\s+登入會員)?',
                            mask.get_text(' ', strip=True)):
                raise NewsError('paywall_or_login')
    if (headline is not None and not headline.get_text(' ', strip=True)
            and container is not None and not container.get_text(' ', strip=True)
            and container.find(['img', 'video', 'audio', 'iframe']) is None):
        raise NewsError('source_article_empty_template')


def einfo_metadata(soup, *, page_url):
    match = re.fullmatch(r"/node/([0-9]+)", urlsplit(page_url or "").path)
    script = soup.find("script", id="__NEXT_DATA__")
    if not match or not script:
        raise NewsError("article_identity_not_verified")
    try:
        post = json.loads(script.get_text())["props"]["pageProps"]["postData"]
        if (not isinstance(post, dict) or post.get("__typename") != "Post"
                or str(post.get("id")) != match[1] or post.get("state") != "published"):
            raise ValueError("not_current_public_post")
        title = post["title"]
        header = soup.find("h1")
        if not isinstance(title, str) or not header or header.get_text(" ", strip=True) != title:
            raise ValueError("headline_identity_mismatch")
        if (post.get("isNewsletter") is not False or re.search(r"環境書摘|電子報", title)
                or post.get("sources") != [] or post.get("writers") != [] or post.get("otherByline")
                or any(post.get(k) for k in ("sourcesInInputOrder", "writersInInputOrder", "translatorsInInputOrder"))):
            raise NewsError("article_rights_or_scope_excluded")
        reporters = post["reporters"]
        if (not isinstance(reporters, list) or not reporters
                or any(not isinstance(r, dict) or not isinstance(r.get("name"), str) or not r["name"].strip() for r in reporters)):
            raise NewsError("article_attribution_not_verified")
        # The published byline identifies the original newsroom. The site-wide
        # CC policy is NOT inherited by syndicated/external/book text.
        credit = soup.select_one("ul[class*='post-credit__CreditList-']")
        credit_text = credit.get_text(" ", strip=True) if credit else ""
        names = [r["name"].strip() for r in reporters]
        if (not all(name in credit_text for name in names)
                or not ("環境資訊中心" in credit_text or re.match(r"^記者\s*[—－-]", credit_text))):
            raise NewsError("article_rights_not_verified")
        clock = post["publishTime"]
        if not isinstance(clock, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(?:Z|[+-][0-9]{2}:[0-9]{2})", clock):
            raise NewsError("invalid_bulk_publication_clock")
        datetime.fromisoformat(clock.replace("Z", "+00:00"))
        return {"headline": title, "author": [{"name": r["name"].strip()} for r in reporters],
                "datePublished": clock, "dateModified": post.get("updatedAt")}, "publisher_default_policy_verified_original_editorial"
    except (KeyError, TypeError, ValueError):
        raise NewsError("article_identity_not_verified") from None


_DATE = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+[0-9]{1,2},\s*[0-9]{4}"


def fed_additional_legacy_release(soup, *, table_headers=False, table_rows=False):
    """Source-backed preformatted transcript and interagency dateline layouts.

    Require independent release markers and bounded direct body containers.
    Footer updates, URL dates, sidebars and arbitrary page-wide text never
    establish a publication clock or body.
    """
    if not soup.body:
        return None
    heads = soup.select('body > h2')
    transcripts = soup.select('body > pre')
    if len(heads) == len(transcripts) == 1:
        stamp = heads[0].get_text(' ', strip=True)
        text = transcripts[0].get_text()
        if (re.fullmatch(_DATE, stamp, re.I) and text.lstrip().startswith('THE WHITE HOUSE')
                and 'Office of the Press Secretary' in text[:250]
                and re.search(r'For Immediate Release\s+' + re.escape(stamp), text[:500], re.I)):
            headline = soup.select_one('body > h3')
            return stamp, transcripts, headline.get_text(' ', strip=True) if headline else ''
    nodes = list(soup.body.find_all(recursive=False))
    for index, node in enumerate(nodes):
        if node.name != 'table' or str(node.get('width')) != '560' or not re.fullmatch(
                r'For immediate release', node.get_text(' ', strip=True), re.I):
            continue
        containers = []
        for candidate in nodes[index + 1:]:
            if candidate.name == 'hr' or candidate.name == 'font' and str(candidate.get('size')) == '-1':
                break
            if candidate.name == 'table' and str(candidate.get('width')) in {'555', '600', '650'}:
                containers.append(candidate)
        if containers:
            lead = containers[0].get_text(' ', strip=True)[:500]
            clock = re.search(r'\bWASHINGTON,\s*D[.]C[.],\s*(' + _DATE + r')\s*[-–—]', lead, re.I)
            if clock:
                return clock.group(1), containers, ''
    if not table_headers:
        return None
    # Verified 2001 layouts put the release marker/date in two adjacent table
    # cells, either inside the article cell or above a separate article table.
    # Match only that complete header, never dates in the body/footer.
    headers = []
    for row in soup.find_all('tr'):
        cells = row.find_all('td', recursive=False)
        if (len(cells) == 2 and re.fullmatch(r'For immediate release', cells[0].get_text(' ', strip=True), re.I)
                and re.fullmatch(_DATE, cells[1].get_text(' ', strip=True), re.I)):
            table = row.find_parent('table')
            if table and str(table.get('width')) in {'564', '600'}:
                headers.append((table, cells[1].get_text(' ', strip=True), row))
    if len(headers) == 1:
        table, stamp, release_row = headers[0]
        parent = table.parent
        following_cells = [cell for row in release_row.find_next_siblings('tr') for cell in row.find_all('td', recursive=False)
                           if cell.get_text(' ', strip=True)] if table_rows else []
        if following_cells:
            # Joint releases put both header and article in one direct table.
            # Select only subsequent rows, not the agency masthead/header.
            candidates = following_cells
            direct_body = False
        elif parent and parent.name == 'td' and parent.find_parent('table') and str(parent.find_parent('table').get('width')) == '600':
            candidates = list(table.find_next_siblings())
            direct_body = False
        elif parent is soup.body:
            candidates = list(table.find_next_siblings())
            direct_body = True
        else:
            return None
        containers = []
        for candidate in candidates:
            text = candidate.get_text(' ', strip=True)
            if (candidate.name == 'hr' or candidate.name == 'font' and str(candidate.get('size')) == '-1'
                    or candidate.name == 'p' and (re.fullmatch(r'(?:[0-9]{4}\s+)?Other announcements', text, re.I)
                       or any(re.fullmatch(r'/boarddocs/press/[a-z]+/[0-9]{4}/?', a.get('href', '')) for a in candidate.find_all('a')))):
                break
            if direct_body and (candidate.name != 'table' or str(candidate.get('width')) not in {'555', '600', '650'}):
                continue
            if text:
                containers.append(candidate)
        if containers:
            headline = next((n for container in containers for n in
                             ([container] if container.name == 'div' and container.get('align') == 'center' else container.select('div[align="center"]'))), None)
            return stamp, containers, headline.get_text(' ', strip=True) if headline else ''
    return None


def fed_bounded_document(soup, *, page_url):
    """Verified minutes and implementation-note containers, never whole page.

    FOMC meeting/event dates and footer revisions are NOT publication clocks.
    A minutes page without a declared release time keeps it unknown.
    """
    path = urlsplit(page_url or '').path
    if re.fullmatch(r'/monetarypolicy/fomcminutes[0-9]{8}[.]htm', path):
        content = soup.select_one('#content #printThis #leftText')
        title = soup.select_one('#content #printThis > h1')
        if (content and title and title.get_text(' ', strip=True) == 'Minutes of the Federal Open Market Committee'
                and re.match(r'^A meeting of the Federal Open Market Committee', content.get_text(' ', strip=True))):
            for link in content.select('a[href="#pagetop"]'):
                link.decompose()
            return None, [content], title.get_text(' ', strip=True)
    if re.fullmatch(r'/newsevents/pressreleases/monetary[0-9]{8}[a-z][0-9]+[.]htm', path):
        header = soup.select_one('#content > .row > .heading.col-xs-12')
        title = header.find('h3') if header else None
        clock = header.select_one('.article__time') if header else None
        stamp = clock.get_text(' ', strip=True) if clock else ''
        containers = soup.select('#content > .row > .col-xs-12:not(.heading)')
        if (title and re.fullmatch(_DATE, stamp, re.I)
                and title.get_text(' ', strip=True) == 'Implementation Note issued ' + stamp and len(containers) == 1
                and 'Decisions Regarding Monetary Policy Implementation' in containers[0].get_text(' ', strip=True)[:120]):
            return stamp, containers, title.get_text(' ', strip=True)
    return None


_FLEX_RELEASE_DATE = _DATE.replace(r'{1,2},', r'{1,2}\s*,')
_SPANISH_RELEASE_DATE = (r'[0-9]{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|'
                         r'agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de|del)\s+[0-9]{4}')


def fed_extended_document(soup, *, page_url, additional_layouts=False, historical_variants=False):
    """Source-contract v8-v10: reviewed article cells and document columns only.

    Keep each declared release string. Meeting dates, titles, PDF metadata and
    footer revisions never become publication observations. A video landing
    page stays distinct from its separately fetched original transcript PDF.
    """
    path = urlsplit(page_url or '').path

    def result(stamp, containers, title, *, timed=False):
        return {'published_at_raw': stamp, 'containers': containers, 'title': title,
                'published_time_basis': ('publisher_declared_release_clock; timezone_abbreviation_preserved' if timed
                    else 'publisher_declared_date_only; no_intraday_time_inferred' if stamp
                    else 'unknown; meeting_date_and_footer_are_not_release_clock')}

    if re.fullmatch(r'/boarddocs/press/(?:[a-z]+/)?[0-9]{4}/[0-9a-z]+/(?:[^/]+[.]htm)?', path) and soup.body:
        # Joint releases use either two date cells or a release/date in one
        # cell, with a contact cell beside it. Only subsequent source rows are
        # body. Do not capture the masthead, footer or a nested contact table.
        headers = []
        for table in soup.select('body > table[width="600"]'):
            for row in table.find_all('tr', recursive=False):
                cells = row.find_all('td', recursive=False)
                if len(cells) != 2:
                    continue
                first, second = (cell.get_text(' ', strip=True) for cell in cells)
                stamp, timed = None, False
                if re.fullmatch('For immediate release', first, re.I) and re.fullmatch(_FLEX_RELEASE_DATE, second, re.I):
                    stamp = second
                elif re.fullmatch('Para diseminación inmediata', first, re.I) and re.fullmatch(_SPANISH_RELEASE_DATE, second, re.I):
                    stamp = second
                else:
                    match = re.fullmatch(r'For immediate release\s+(' + _FLEX_RELEASE_DATE + ')', first, re.I)
                    if match:
                        stamp = match[1]
                if (not stamp and additional_layouts and re.fullmatch(_FLEX_RELEASE_DATE, second, re.I)
                        and re.fullmatch(r'For release at [0-9]{1,2}(?::[0-9]{2})? [ap][.]m[.] (?:EDT|EST)', first, re.I)):
                    # The actual release declaration is two cells. Preserve
                    # both observed strings, including its source time zone.
                    stamp, timed = first + '\n' + second, True
                if stamp:
                    headers.append((row, stamp, timed))
        if len(headers) == 1:
            row, stamp, timed = headers[0]
            containers = [cell for sibling in row.find_next_siblings('tr')
                          for cell in sibling.find_all('td', recursive=False) if cell.get_text(' ', strip=True)]
            if containers:
                heading = next((n for cell in containers for n in cell.select('div[align="center"]')
                                if not additional_layouts or n.get_text(' ', strip=True)), None)
                title = heading.get_text(' ', strip=True) if heading else soup.title.get_text(' ', strip=True) if soup.title else ''
                return result(stamp, containers, title, timed=timed)
        # Spanish legacy releases declare Fecha above, not in the title/URL.
        clocks = [n for n in soup.body.find_all('font', recursive=False)
                  if re.fullmatch(r'Fecha:\s*' + _SPANISH_RELEASE_DATE, n.get_text(' ', strip=True), re.I)]
        if len(clocks) == 1:
            containers = []
            for node in clocks[0].find_next_siblings():
                if node.name == 'hr' or node.name == 'font' and str(node.get('size')) == '-1':
                    break
                if node.name == 'table' and str(node.get('width')) in {'555', '600', '650'} and node.get_text(' ', strip=True):
                    containers.append(node)
            if containers and soup.title:
                stamp = re.sub(r'^Fecha:\s*', '', clocks[0].get_text(' ', strip=True), flags=re.I)
                return result(stamp, containers, soup.title.get_text(' ', strip=True))
        # Later policy statements nest the release header inside their one
        # 600px article cell. Bound its following siblings, not the whole body.
        declarations = [n for table in soup.select('body > table[width="600"]')
                        for n in table.select('td > font')
                        if re.fullmatch(r'Release Date:\s*' + _FLEX_RELEASE_DATE, n.get_text(' ', strip=True), re.I)]
        if len(declarations) == 1 and soup.title:
            declaration = declarations[0]
            containers = [n for n in declaration.find_next_siblings() if n.get_text(' ', strip=True)]
            if containers:
                stamp = re.sub(r'^Release Date:\s*', '', declaration.get_text(' ', strip=True), flags=re.I)
                return result(stamp, containers, soup.title.get_text(' ', strip=True))
        if additional_layouts and soup.title:
            # Public English/Spanish declarations sometimes include a
            # translation link in the same font header. Only that link is
            # excluded from the clock; no title/URL/footer date is substituted.
            declarations = []
            for node in soup.body.find_all('font', recursive=False):
                clock_text = ' '.join(str(n).strip() for n in node.strings
                    if str(n).strip() and not any(p.name == 'a' for p in n.parents if p is not node))
                match = re.fullmatch(r'(?:FOR IMMEDIATE RELEASE\s+(' + _FLEX_RELEASE_DATE
                    + r')|Comunicado de prensa\s+(' + _SPANISH_RELEASE_DATE + r')|For Release:\s*('
                    + _FLEX_RELEASE_DATE + r'(?: at [0-9]{1,2}(?::[0-9]{2})? [ap][.]m[.] Eastern Time)?))', clock_text, re.I)
                if match:
                    stamp = next(value for value in match.groups() if value)
                    declarations.append((node, stamp))
            if len(declarations) == 1:
                declaration, stamp = declarations[0]
                containers = []
                for node in declaration.find_next_siblings():
                    if node.name == 'hr' or node.name == 'font' and str(node.get('size')) == '-1':
                        break
                    if node.name == 'table' and str(node.get('width')) in {'555', '600', '650'} and node.get_text(' ', strip=True):
                        containers.append(node)
                if containers:
                    info = result(stamp, containers, soup.title.get_text(' ', strip=True), timed=' Eastern Time' in stamp)
                    if ' Eastern Time' in stamp:
                        info['published_time_basis'] = 'publisher_declared_release_clock; source_Eastern_Time_text_preserved'
                    return info

    if re.fullmatch(r'/monetarypolicy/fomcminutes[0-9]{8}[.]htm', path):
        scopes = soup.select('#content #printThis')
        if len(scopes) == 1:
            scope = scopes[0]
            titles = scope.select(':scope > h1')
            columns = scope.select(':scope > #TwoColumns')
            if (len(titles) == len(columns) == 1
                    and titles[0].get_text(' ', strip=True) == 'Minutes of the Federal Open Market Committee'):
                columns = columns[0]
                left = columns.select(':scope > #leftText')
                if len(left) == 1 and re.match(r'^A (?:joint )?meeting of the Federal Open Market Committee', left[0].get_text(' ', strip=True)):
                    for link in left[0].select('a[href="#pagetop"]'):
                        link.decompose()
                    return result(None, left, titles[0].get_text(' ', strip=True))
                introductions = [n for n in columns.find_all('p', recursive=False)
                    if re.match(r'^A (?:joint )?meeting of the Federal Open Market Committee', n.get_text(' ', strip=True))]
                if len(left) == len(introductions) == 1 and left[0].get_text(' ', strip=True).startswith('PRESENT:'):
                    for link in left[0].select('a[href="#pagetop"]'):
                        link.decompose()
                    return result(None, [introductions[0], left[0]], titles[0].get_text(' ', strip=True))

    if re.fullmatch(r'/monetarypolicy/fomcprojtabl[0-9]{8}[.]htm', path):
        scopes = soup.select('#content > .row > .col-xs-12.col-sm-8.col-md-8')
        if len(scopes) == 1:
            scope = scopes[0]
            headings = scope.find_all('h3', recursive=False)
            clocks = [n.get_text(' ', strip=True) for n in scope.find_all('p', recursive=False)
                if re.fullmatch(r'For release at [0-9]{1,2}:[0-9]{2} [ap][.]m[.], (?:EDT|EST), ' + _DATE,
                                n.get_text(' ', strip=True), re.I)]
            figures = [n.get_text(' ', strip=True) for n in scope.find_all(['h4', 'h5'] if historical_variants else 'h4')]
            summary = any(n == 'Summary of Economic Projections' or historical_variants and n.startswith(
                'Advance release of table 1 of the Summary of Economic Projections') for n in figures)
            if (headings and re.fullmatch(_DATE + r': FOMC Projections materials, accessible version', headings[0].get_text(' ', strip=True), re.I)
                    and len(clocks) == 1 and scope.select('.data-table table')
                    and summary):
                return result(clocks[0], [scope], headings[0].get_text(' ', strip=True), timed=True)
            if additional_layouts and headings and not clocks and scope.select('.data-table table'):
                title = headings[0].get_text(' ', strip=True)
                dated_title = re.fullmatch(_DATE + r': FOMC Projections materials, accessible version', title, re.I)
                early_title = (title == 'Accessible Version' and soup.title
                    and re.fullmatch(r'The Fed - ' + _DATE + (r'(?: Projection materials|: FOMC Projections materials)' if historical_variants
                                                            else r' Projection materials') + r', accessible version',
                                     soup.title.get_text(' ', strip=True), re.I))
                if ((dated_title or early_title) and any(
                        n.startswith(('Figure 1. Central tendencies and ranges of economic projections',
                                      'Figure 1. Medians, central tendencies, and ranges of economic projections',
                                      'Advance release of table 1 of the Summary of Economic Projections'))
                        or n == 'Summary of Economic Projections' for n in figures)):
                    # Older accessible tables have an observation/meeting date
                    # but no release declaration. Keep their valuable source
                    # numbers with an unknown publication, never discard them
                    # or backdate them to a heading/footer/HTTP modification.
                    return result(None, [scope], soup.title.get_text(' ', strip=True) if early_title else title)

    if re.fullmatch(r'/newsevents/pressreleases/monetary[0-9]{8}[a-z][0-9]+[.]htm', path):
        header_selector = '#content > .row > .heading.col-xs-12'
        if not historical_variants:
            header_selector += '.col-sm-8'
        header = soup.select_one(header_selector)
        title = header.find('h3') if header else None
        clock = header.select_one('.article__time') if header else None
        stamp = clock.get_text(' ', strip=True) if clock else ''
        body_selector = ('#content > .row > .col-xs-12:not(.heading):not(.col-sm-4)' if historical_variants
                         else '#content > .row > .col-xs-12.col-sm-8:not(.heading)')
        containers = soup.select(body_selector)
        if (not additional_layouts and title and re.fullmatch(_DATE, stamp, re.I)
                and title.get_text(' ', strip=True) == 'Implementation Note issued ' + stamp and len(containers) == 1
                and containers[0].get_text(' ', strip=True).startswith('Decisions Regarding Monetary Policy Implementation')):
            return result(stamp, containers, title.get_text(' ', strip=True))
        if additional_layouts and title and re.fullmatch(_DATE, stamp, re.I) and len(containers) == 1:
            title_match = re.fullmatch(r'Implementation Note issued (' + _DATE + ')', title.get_text(' ', strip=True), re.I)
            if title_match and containers[0].get_text(' ', strip=True).startswith('Decisions Regarding Monetary Policy Implementation'):
                # A native zero-padded display day and the unpadded title
                # denote the same calendar date. Compare, do not rewrite the
                # declaration retained as published_at_raw.
                normalized_stamp = re.sub(r'\s+,', ',', stamp)
                normalized_title = re.sub(r'\s+,', ',', title_match[1])
                try:
                    same_day = datetime.strptime(normalized_stamp, '%B %d, %Y') == datetime.strptime(normalized_title, '%B %d, %Y')
                except ValueError:
                    same_day = False
                if same_day:
                    return result(stamp, containers, title.get_text(' ', strip=True))

    conference = re.fullmatch(r'/monetarypolicy/fomcpresconf([0-9]{8})[.]htm', path)
    if conference:
        scope = soup.select_one('#content #printThis')
        title = scope.select_one(':scope > h1') if scope else None
        if (title and re.fullmatch(r'FOMC: Press Conference on ' + _DATE, title.get_text(' ', strip=True))
                and scope.select('#leftText3 video') and any(
                    a.get('href') == '/mediacenter/files/FOMCpresconf' + conference[1] + '.pdf'
                    and a.get_text(' ', strip=True).startswith('Press Conference Transcript') for a in scope.select('a[href]'))):
            raise NewsError('source_document_landing_page')
    return None
