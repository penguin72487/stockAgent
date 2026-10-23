"""UpMedia's public modern editor, not a legacy-URL converter or news API.

Use the literal current article identity and visible byline/clock.  A publisher
declaration does not independently establish copyright or historical first
release; those limitations travel with the retained original and extracted row.
No JavaScript, linked media, AI tools, index, or author page is requested.
"""
from __future__ import annotations

from datetime import datetime
from collections import Counter
from html.parser import HTMLParser
import hashlib
import json
import re
import unicodedata
from urllib.parse import unquote, urlsplit

from bs4 import Comment, Tag

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


UPMEDIA_LAYOUT = 'upmedia_public_modern_article_v1'
UPMEDIA_LAYOUT_V2 = 'upmedia_public_modern_article_v2'
UPMEDIA_LAYOUT_V3 = 'upmedia_public_modern_article_v3'
UPMEDIA_LAYOUT_V4 = 'upmedia_public_modern_article_v4'
UPMEDIA_LAYOUT_VERSIONS = {UPMEDIA_LAYOUT: 1, UPMEDIA_LAYOUT_V2: 2, UPMEDIA_LAYOUT_V3: 3, UPMEDIA_LAYOUT_V4: 4}
UPMEDIA_PATH_PATTERN = r'^/tw/(?:focus|commentary|lifestyle)/[a-z-]+/[1-9][0-9]*$'
UPMEDIA_BODY_SELECTOR = ':scope > .news-box-text'
UPMEDIA_AUTHOR_SELECTOR = ':scope > .publish > .publish-text > .author > a'
UPMEDIA_AUTHOR_SELECTOR_V2 = (UPMEDIA_AUTHOR_SELECTOR +
    ', :scope > .publish > .publish-text > a:has(> span.author.au2)')
UPMEDIA_AUTHOR_SELECTOR_V3 = (UPMEDIA_AUTHOR_SELECTOR +
    ', :scope > .publish > .publish-text > a:has(> span.author)')
UPMEDIA_EXCLUSIONS = ('.ad-detail', '.rss_close', '.news-foot')
UPMEDIA_BOUNDARY_QUESTION = 'upmedia_current_native_identity_credit_boundary_v1'
UPMEDIA_HEADER_QUESTION = 'upmedia_missing_publication_header_geometry_v1'
UPMEDIA_EDITOR_QUESTION = 'upmedia_missing_editor_and_profile_transport_geometry_v1'
UPMEDIA_REMAINING_QUESTION = 'upmedia_missing_profile_navigation_and_original_JSON_context_v1'
# Only the exact reviewed diagnostic -> canonical source/auth transition may
# retain transport control identity.  No generic authorization inheritance.
UPMEDIA_HTTP_BINDING = (
    '5fa1c098a744ffff6c3427956aded988e1fba44ee9604a739bdcaea30140d48d',
    '2318a58b9f63b0c9b12b56c253d1f7af2c7a45d72f8d0967c4ce8a402b99ef17',
    '99106cf1eb1fb69295a06506ad8bfddf08f0baa01ed997283c38ac6b34a8b10f',
    '96c712d6bf051c7690fdbb106f29a40f3a25d52520a454bd8faf6b83d5b96ffe',
)
# Populated only after reviewing the complete v2 source/auth entry. A changed
# article interpretation must not create a fresh HTTP/refusal allowance.
UPMEDIA_V2_HTTP_BINDING = (
    '5fa1c098a744ffff6c3427956aded988e1fba44ee9604a739bdcaea30140d48d',
    '2c6d13d39c4015385fd15f3557c62dcca5203b6989fb8a64d1847f77db772ae1',
    '540ee77ec04419966e7ec88593e49155d124e8a15abb8f8185d460fc8da58745',
    '99106cf1eb1fb69295a06506ad8bfddf08f0baa01ed997283c38ac6b34a8b10f',
    '96c712d6bf051c7690fdbb106f29a40f3a25d52520a454bd8faf6b83d5b96ffe',
)
# Filled only from the reviewed full v3 candidate config before live admission.
UPMEDIA_V3_HTTP_BINDING = (
    '2c6d13d39c4015385fd15f3557c62dcca5203b6989fb8a64d1847f77db772ae1',
    'e243fcce61cf684b62b7876108296671bc1837f16f35352b1b4079015f21011e',
    '540ee77ec04419966e7ec88593e49155d124e8a15abb8f8185d460fc8da58745',
    '99106cf1eb1fb69295a06506ad8bfddf08f0baa01ed997283c38ac6b34a8b10f',
    '96c712d6bf051c7690fdbb106f29a40f3a25d52520a454bd8faf6b83d5b96ffe',
)
# Filled from the exact reviewed v4 source; parser repairs do not reset HTTP.
UPMEDIA_V4_HTTP_BINDING = (
    'e243fcce61cf684b62b7876108296671bc1837f16f35352b1b4079015f21011e',
    '0dc6fd796ac1ccdc1d0c5b286f9a0e20eb69fede9216cf055537c34b5409ee76',
    '540ee77ec04419966e7ec88593e49155d124e8a15abb8f8185d460fc8da58745',
    '99106cf1eb1fb69295a06506ad8bfddf08f0baa01ed997283c38ac6b34a8b10f',
    '96c712d6bf051c7690fdbb106f29a40f3a25d52520a454bd8faf6b83d5b96ffe',
)

# Editorial roles literally declared by the retained 2026-10-22 /about page.
# This is current publisher attribution, never a historical employment witness.
_REVIEWED_EDITORIAL_STAFF = frozenset({
    '蔡慧貞', '楊毅', '李濠仲', '趙雅芬', '吳文元', '仇佩芬', '呂品逸',
    '殷偵維', '王晨芝', '卓琦', '陳愷巨', '王侑聖', '朱明', '顏振凱',
    '何醒邦', '胡宥心', '沈粲家', '林宣佑', '韓政燕', '劉孜芹', '李浩宇',
    '陳玟穎', '林宜萱', '李雨勳', '吳維書', '徐郁雯', '王穎禛',
    '林冠伶', '周羿庭', '廖柏昀',
})
_PROSE_TAGS = frozenset({
    'p', 'div', 'span', 'br', 'strong', 'b', 'em', 'i', 'u', 's', 'a', 'img',
    'h2', 'h3', 'h4', 'ul', 'ol', 'li', 'blockquote', 'sub', 'sup',
    'figure', 'figcaption', 'table', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'caption',
})
_REPRINT = re.compile(
    r'(?:本文|本篇文章|本篇|文章|新聞稿)(?:為|由|係)?(?:轉載|轉自)|'
    r'(?:全文|原文)轉載|(?:轉載|轉自|授權轉載)(?:自|於|：|:)|'
    r'（中央社記者[^）]{1,100}）')


def _children(node):
    for child in node.children:
        if isinstance(child, Tag):
            yield child
        elif isinstance(child, Comment):
            continue
        elif str(child).strip():
            raise NewsError('article_identity_not_verified')


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None


def upmedia_evidence_source(source, *, layout=UPMEDIA_LAYOUT):
    selector = {UPMEDIA_LAYOUT: UPMEDIA_AUTHOR_SELECTOR, UPMEDIA_LAYOUT_V2: UPMEDIA_AUTHOR_SELECTOR_V2,
        UPMEDIA_LAYOUT_V3: UPMEDIA_AUTHOR_SELECTOR_V3, UPMEDIA_LAYOUT_V4: UPMEDIA_AUTHOR_SELECTOR_V3}.get(layout)
    return bool(source.provider == 'upmedia' and source.allowed and source.request_enabled
        and selector and source.embedded_article_layout == layout and source.known_url_only
        and source.acquisition_mode == 'continuous' and source.domains == ('www.upmedia.mg',)
        and source.authorization.get('kind') == 'personal_research'
        and source.article_path_pattern == UPMEDIA_PATH_PATTERN
        and source.selectors == (UPMEDIA_BODY_SELECTOR,)
        and source.author_selectors == (selector,)
        and source.require_author_attribution and source.require_configured_container)


def upmedia_boundary_metadata(soup, *, adapter_contract_version=1, raw_payload=None):
    """Bounded same-document structural facts, without names or article prose.

    A missing-body diagnosis must not need a new HTTP owner or weaken article
    admission. Hash native title/credit declarations; report shape and exact
    reviewed-role comparisons, never turn these observations into a body.
    """
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3, 4}:
        raise ValueError('unsupported_upmedia_metadata_contract')

    def digest(value):
        return hashlib.sha256(str(value).encode()).hexdigest()

    def shape(node):
        return {'tag': node.name, 'classes': [str(c)[:64] for c in node.get('class', [])[:8]],
            'text_characters': len(node.get_text(' ', strip=True)), 'hidden': node.has_attr('hidden'),
            'aria_hidden': str(node.get('aria-hidden', '')).lower() == 'true',
            'display_none': 'display:none' in re.sub(r'\s+', '', str(node.get('style', ''))).lower()}

    def tag_counts(counts):
        return {name[:64]: counts[name] for name in sorted(counts)[:64]}, len(counts) > 64

    scopes = soup.select('div.news-box')
    result = {'contract_version': adapter_contract_version, 'scope_nodes': len(scopes),
        'raw_response_retained': False, 'names_retained': False, 'article_prose_retained': False}
    if len(scopes) != 1:
        return result
    scope = scopes[0]
    result['scope_children'] = [shape(n) for n in scope.find_all(recursive=False)[:16]]
    headings, editors = scope.select(':scope > h1'), scope.select(UPMEDIA_BODY_SELECTOR)
    selector = {1: UPMEDIA_AUTHOR_SELECTOR, 2: UPMEDIA_AUTHOR_SELECTOR_V2,
        3: UPMEDIA_AUTHOR_SELECTOR_V3, 4: UPMEDIA_AUTHOR_SELECTOR_V3}[adapter_contract_version]
    links = scope.select(selector)
    names = [link.get_text(' ', strip=True) for link in links]
    result.update(heading_nodes=len(headings), editor_nodes=len(editors), byline_links=len(links),
        byline_name_sha256=[digest(name) for name in names[:4]],
        byline_profile_matches=[link.get('href') == '/author/' + name for link, name in zip(links[:4], names[:4])],
        single_house_desk=names == ['上報快訊'],
        paired_house_desk=len(names) == 2 and names[0] == '上報快訊',
        single_reviewed_staff=len(names) == 1 and names[0] in _REVIEWED_EDITORIAL_STAFF)
    if len(headings) == 1:
        result['visible_title_sha256'] = digest(headings[0].get_text(' ', strip=True))
    declarations = []
    for script in soup.select('script[type="application/ld+json"]')[:16]:
        try:
            declarations.extend(obj for obj in _objects(json.loads(script.get_text())) if obj.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            result['invalid_article_declaration'] = True
    result['schema_article_nodes'] = len(declarations)
    if len(declarations) == 1:
        declared = declarations[0]
        author = declared.get('author')
        result['schema_headline_sha256'] = digest(declared.get('headline'))
        result['headline_matches_visible'] = (len(headings) == 1
            and declared.get('headline') == headings[0].get_text(' ', strip=True) + ' -- 上報')
        if isinstance(author, dict):
            name = '／'.join(names)
            profile = 'https://www.upmedia.mg/author/' + name
            result.update(schema_author_name_sha256=digest(author.get('name')),
                schema_author_matches_visible=author.get('name') == name,
                schema_author_is_person=author.get('@type') == 'Person',
                schema_profile_matches=author.get('url') == profile,
                head_profile_matches=any(n.get('content') == profile for n in soup.select('meta[property="article:author"]')))
    if len(editors) == 1:
        body = editors[0]
        counts = Counter(n.name for n in body.find_all())
        result['editor_tags'], result['editor_tags_truncated'] = tag_counts(counts)
        result['unsupported_editor_tags'] = [name[:64] for name in sorted(set(counts) - _PROSE_TAGS)[:64]]
        exclusions = body.select('.ad-detail, .rss_close, .news-foot')
        result['exclusion_nodes'] = len(exclusions)
        result['exclusions'] = []
        for node in exclusions[:16]:
            descendants, truncated = tag_counts(Counter(n.name for n in node.find_all()))
            text = node.get_text('', strip=True)
            result['exclusions'].append({**shape(node), 'direct_editor_child': node.parent is body,
                'descendant_tags': descendants, 'descendant_tags_truncated': truncated,
                'related_prefix_known': text.startswith(('（延伸閱讀：', '（熱門點閱：')),
                'related_suffix_known': text.endswith('）')})
    if adapter_contract_version >= 2:
        # Same fixed boundary question, not a new observation allowance. The
        # v1 result/absence of publication_header remains exactly unchanged.
        result['publication_header'] = upmedia_publication_metadata(soup)
    if adapter_contract_version >= 3:
        # New failed heads carry the newly needed facts on their first current
        # boundary observation, not another question/HTTP request per field.
        result['editor_transport'] = upmedia_editor_metadata(soup)
        result['remaining_context'] = upmedia_remaining_metadata(soup, raw_payload=raw_payload)
    return result


def upmedia_publication_metadata(soup):
    """Missing header geometry only; never retain names, prose or linked URLs.

    The old boundary question did not retain this subtree. Bound every list and
    expose exact declaration comparisons rather than guessing an author from a
    CSS class or interpreting a publisher desk as a verified natural person.
    """
    publications = soup.select('div.news-box > .publish > .publish-text')
    result = {'contract_version': 1, 'publication_text_nodes': len(publications),
        'raw_response_retained': False, 'names_retained': False, 'article_prose_retained': False}
    if len(publications) != 1:
        return result
    root = publications[0]
    nodes = [root, *root.find_all(limit=64)]
    result['nodes_truncated'] = len(nodes) > 64
    nodes = nodes[:64]
    indices = {id(node): index for index, node in enumerate(nodes)}
    declarations = []
    for script in soup.select('script[type="application/ld+json"]')[:16]:
        try:
            declarations.extend(obj for obj in _objects(json.loads(script.get_text()))
                                if obj.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            result['invalid_article_declaration'] = True
    result['schema_article_nodes'] = len(declarations)
    author = declarations[0].get('author') if len(declarations) == 1 else None
    author = author if isinstance(author, dict) else {}
    declared_name = author.get('name')
    result['nodes'] = []
    for index, node in enumerate(nodes):
        name = node.get_text(' ', strip=True)
        classes = node.get('class', [])
        result['nodes'].append({'index': index, 'parent': indices.get(id(node.parent)),
            'tag': node.name[:64],
            'classes': [c if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9-]{0,63}', c) else 'unrepresented-class'
                        for c in classes[:8]], 'classes_truncated': len(classes) > 8,
            'text_characters': len(name), 'text_sha256': hashlib.sha256(name.encode()).hexdigest(),
            'hidden': node.has_attr('hidden'), 'aria_hidden': str(node.get('aria-hidden', '')).lower() == 'true',
            'display_none': 'display:none' in re.sub(r'\s+', '', str(node.get('style', ''))).lower(),
            'direct_text_characters': sum(len(str(child).strip()) for child in node.children
                                         if not isinstance(child, (Tag, Comment))),
            'schema_name_matches': bool(name) and name == declared_name,
            'literal_author_profile_matches_text': node.name == 'a' and bool(name)
                and node.get('href') == '/author/' + name,
            'schema_profile_matches_link': node.name == 'a' and isinstance(author.get('url'), str)
                and 'https://www.upmedia.mg' + str(node.get('href', '')) == author['url'],
            'house_desk': name == '上報快訊', 'current_reviewed_staff': name in _REVIEWED_EDITORIAL_STAFF})
    result['schema_author_is_person'] = author.get('@type') == 'Person'
    result['head_profile_matches_schema'] = isinstance(author.get('url'), str) and any(
        node.get('content') == author['url'] for node in soup.select('meta[property="article:author"]'))
    return result


def upmedia_editor_metadata(soup):
    """Previously unretained exclusion/URL-encoding facts, never accepting prose.

    Boundary/header questions report aggregate geometry, not script boundaries,
    related-label whitespace or encoded profile agreement. This distinct fixed
    question requires their immutable prior proof at both queue admissions.
    Attribute values, script text, names and unknown reading labels stay hashed.
    """
    def digest(value):
        return hashlib.sha256(str(value).encode()).hexdigest()

    def shape(node):
        classes = node.get('class', [])
        return {'tag': node.name[:64], 'classes': [c if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9-]{0,63}', c)
            else 'unrepresented-class' for c in classes[:8]], 'classes_truncated': len(classes) > 8,
            'hidden': node.has_attr('hidden'), 'aria_hidden': str(node.get('aria-hidden', '')).lower() == 'true',
            'display_none': 'display:none' in re.sub(r'\s+', '', str(node.get('style', ''))).lower()}

    scopes = soup.select('div.news-box')
    result = {'contract_version': 1, 'scope_nodes': len(scopes),
        'raw_response_retained': False, 'names_retained': False, 'article_prose_retained': False,
        'scripts_requested_or_executed': False, 'linked_profiles_requested': False}
    if len(scopes) != 1:
        return result
    scope = scopes[0]
    editors = scope.select(UPMEDIA_BODY_SELECTOR)
    links = scope.select(UPMEDIA_AUTHOR_SELECTOR_V2)
    result.update(editor_nodes=len(editors), byline_links=len(links), byline_links_truncated=len(links) > 4)
    result['profiles'] = []
    for link in links[:4]:
        name, href = link.get_text(' ', strip=True), link.get('href')
        href = href if isinstance(href, str) else ''
        try:
            parts = urlsplit(href)
        except ValueError:
            parts = None
        try:
            decoded = unquote(href, encoding='utf-8', errors='strict')
        except UnicodeError:
            decoded = None
        result['profiles'].append({**shape(link), 'name_sha256': digest(name), 'href_sha256': digest(href),
            'href_characters': len(href), 'relative_author_path': parts is not None and not parts.scheme and not parts.netloc
                and parts.path.startswith('/author/') and not parts.query and not parts.fragment,
            'literal_matches_text': href == '/author/' + name,
            'decoded_once_matches_text': decoded == '/author/' + name,
            'decoded_once_utf8_valid': decoded is not None,
            'has_percent_escape': bool(re.search(r'%[0-9a-fA-F]{2}', href)),
            'url_syntax_valid': parts is not None,
            'has_query': parts is not None and bool(parts.query), 'has_fragment': parts is not None and bool(parts.fragment),
            'has_backslash': '\\' in href, 'outer_whitespace': href != href.strip(),
            'child_tags': [n.name[:64] for n in link.find_all(limit=16)]})
    result['declaration_syntax'] = []
    declarations = soup.select('script[type="application/ld+json"]')
    result['declarations_truncated'] = len(declarations) > 16
    for script in declarations[:16]:
        value = script.get_text()
        syntax = {'characters': len(value), 'sha256': digest(value), 'valid_json': False}
        try:
            json.loads(value)
        except json.JSONDecodeError as error:
            kinds = {'Invalid control character at', 'Expecting value', 'Expecting property name enclosed in double quotes',
                "Expecting ',' delimiter", "Expecting ':' delimiter", 'Extra data', 'Unterminated string starting at',
                'Invalid \\escape', 'Invalid \\uXXXX escape'}
            syntax.update(error_kind=error.msg if error.msg in kinds else 'other',
                error_offset=error.pos, error_line=error.lineno, error_column=error.colno,
                invalid_control_at_error=error.pos < len(value) and ord(value[error.pos]) < 32,
                syntax_classes_near_error=[('whitespace' if c.isspace() else 'quote' if c == '"' else
                    'escape' if c == '\\' else 'punctuation' if c in '{}[],:;' else 'other')
                    for c in value[max(0, error.pos - 8):error.pos + 8]])
        except (TypeError, RecursionError):
            syntax['error_kind'] = 'unsupported_json_structure'
        else:
            syntax['valid_json'] = True
        result['declaration_syntax'].append(syntax)
    if len(editors) != 1:
        return result
    body = editors[0]
    exclusions = body.select('.ad-detail, .rss_close, .news-foot')
    result.update(exclusion_nodes=len(exclusions), exclusions_truncated=len(exclusions) > 16, exclusions=[])
    for node in exclusions[:16]:
        children = node.find_all(limit=64)
        text = node.get_text('', strip=True)
        normalized = re.sub(r'\s+', '', text)
        label = re.match(r'^[（(【\[]?(延伸閱讀|熱門點閱|推薦閱讀|延伸讀物|看更多|相關閱讀)[：:】\]]?', normalized)
        record = {**shape(node), 'direct_editor_child': node.parent is body,
            'text_characters': len(text), 'text_sha256': digest(text),
            'descendant_tags': dict(sorted(Counter(n.name for n in children).items())),
            'descendants_truncated': len(children) == 64,
            'reading_label': label.group(1) if label else 'unclassified',
            'normalized_fullwidth_parentheses': normalized.startswith('（') and normalized.endswith('）'),
            'normalized_known_prefix': normalized.startswith(('（延伸閱讀：', '（熱門點閱：')),
            'literal_known_prefix': text.startswith(('（延伸閱讀：', '（熱門點閱：')),
            'scripts': []}
        scripts = node.select('script')
        record['scripts_truncated'] = len(scripts) > 8
        for script in scripts[:8]:
            script_type = str(script.get('type', '')).lower().strip()
            src = str(script.get('src', ''))
            record['scripts'].append({'type': script_type if script_type in {
                '', 'text/javascript', 'application/javascript', 'application/ld+json', 'application/json'} else 'other',
                'direct_exclusion_child': script.parent is node,
                'within_ad_detail': any('ad-detail' in p.get('class', []) for p in script.parents if isinstance(p, Tag)),
                'has_src': bool(src), 'src_sha256': digest(src),
                'text_characters': len(script.get_text()), 'text_sha256': digest(script.get_text()),
                'attribute_names': [key if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9:-]{0,63}', key)
                    else 'unrepresented-attribute' for key in sorted(script.attrs)[:16]]})
        result['exclusions'].append(record)
    return result


def upmedia_remaining_metadata(soup, *, raw_payload=None):
    """Unretained v3 rejection facts, not names, text or a new body permit.

    Use the actual no-au2 selector; distinguish original JSON controls from
    HTML newline normalization and navigation punctuation from linked titles.
    Every list is bounded. No linked page, script or accepting parse is run.
    """
    def digest(value):
        return hashlib.sha256(value.encode()).hexdigest()

    def punctuation(value):
        return [ord(c) for c in value if unicodedata.category(c).startswith('P')][:64]

    result = {'contract_version': 1, 'raw_response_retained': False, 'names_retained': False,
        'article_prose_retained': False, 'linked_resources_requested': False, 'scripts_executed': False}
    scopes = soup.select('div.news-box')
    result['scope_nodes'] = len(scopes)
    links = scopes[0].select(UPMEDIA_AUTHOR_SELECTOR_V3) if len(scopes) == 1 else []
    result.update(byline_links=len(links), profiles_truncated=len(links) > 4, profiles=[])
    for link in links[:4]:
        name, value = link.get_text(' ', strip=True), link.get('href')
        href = value if isinstance(value, str) else ''
        trimmed = href.strip(' \t\r\n')
        try:
            parts = urlsplit(trimmed)
        except ValueError:
            parts = None
        kind = ('absent' if value is None else 'empty' if not trimmed else
            'fragment_only' if trimmed.startswith('#') else 'root' if trimmed == '/' else
            'relative_author' if parts and not parts.scheme and not parts.netloc and parts.path.startswith('/author/') else
            'absolute_same_host_author' if parts and parts.scheme == 'https' and parts.netloc == 'www.upmedia.mg'
                and parts.path.startswith('/author/') else 'other')
        result['profiles'].append({'href_kind': kind, 'href_present': link.has_attr('href'),
            'href_characters': len(href), 'href_sha256': digest(href), 'name_sha256': digest(name),
            'literal_matches_text': href == '/author/' + name,
            'outer_ASCII_trimmed_matches_text': trimmed == '/author/' + name,
            'url_syntax_valid': parts is not None, 'query_present': bool(parts and parts.query),
            'fragment_present': bool(parts and parts.fragment), 'house_desk': name == '上報快訊',
            'percent_escape_present': bool(re.search(r'%[0-9a-fA-F]{2}', href))})
    related = soup.select('div.news-box > .news-box-text .rss_close')
    result.update(related_nodes=len(related), related_truncated=len(related) > 16, related=[])
    for node in related[:16]:
        written = ''.join(str(t) for t in node.find_all(string=True) if not isinstance(t, Comment)
            and not any(p.name == 'a' for p in t.parents if p is not node))
        label = re.sub(r'\s+', '', written)
        try:
            _related_label(node)
        except NewsError:
            accepted = False
        else:
            accepted = True
        labels = [s for s in ('延伸閱讀', '熱門點閱', '推薦閱讀', '延伸讀物', '看更多', '相關閱讀') if s in label]
        anchors = node.select('a')
        result['related'].append({'tag': node.name, 'outside_anchor_characters': len(written),
            'outside_anchor_sha256': digest(written), 'punctuation_codepoints': punctuation(label),
            'punctuation_truncated': sum(unicodedata.category(c).startswith('P') for c in label) > 64,
            'CJK_run_lengths': [len(v) for v in re.findall(r'[\u3400-\u9fff]+', label)[:16]],
            'known_reading_labels': labels, 'v3_label_grammar_valid': accepted, 'anchor_nodes': len(anchors),
            'anchor_end_punctuation_codepoints': [punctuation(a.get_text('', strip=True)[-4:]) for a in anchors[:4]]})
    scripts = soup.select('script[type="application/ld+json"]')
    result.update(original_JSON_available=False, original_JSON_matches_DOM=False, declaration_syntax=[])
    try:
        originals = _original_json_scripts(raw_payload)
    except (ValueError, UnicodeError):
        return result
    result['original_JSON_available'] = True
    result['original_JSON_matches_DOM'] = len(originals) == len(scripts) and all(
        raw.replace('\r\n', '\n').replace('\r', '\n') == node.get_text() for raw, node in zip(originals, scripts))
    result['declarations_truncated'] = len(originals) > 16
    from downloader.news_fulltext_videoland import _json_control_context, _description_lf_repair
    for raw in originals[:16]:
        syntax = {'characters': len(raw), 'sha256': digest(raw), 'valid_JSON': False,
            'literal_control_counts': {str(n): raw.count(chr(n)) for n in range(32) if chr(n) in raw}}
        try:
            json.loads(raw)
        except json.JSONDecodeError as error:
            control = ord(raw[error.pos]) if error.pos < len(raw) and ord(raw[error.pos]) < 32 else None
            syntax.update(error_offset=error.pos, error_line=error.lineno, error_column=error.colno,
                control_codepoint_at_error=control, original_field_context=_json_control_context(raw, error))
            _, proof = _description_lf_repair(raw, error)
            syntax['existing_description_LF_repair'] = proof
        except (ValueError, TypeError, RecursionError):
            syntax['unsupported_JSON_structure'] = True
        else:
            syntax['valid_JSON'] = True
        result['declaration_syntax'].append(syntax)
    return result


def _related_label(node, *, version=3):
    """An explicit native navigation role, with no unlinked prose discarded."""
    if node.name not in {'div', 'h2', 'h3', 'h4'} or not node.select('a[href]'):
        raise NewsError('article_identity_not_verified')
    if any(child.name not in {'h2', 'h3', 'h4', 'strong', 'a', 'span', 'br'} for child in node.find_all()):
        raise NewsError('article_identity_not_verified')
    written = ''.join(str(text) for text in node.find_all(string=True) if not isinstance(text, Comment)
        and not any(p.name == 'a' for p in text.parents if p is not node))
    label = re.sub(r'\s+', '', written)
    if version >= 4:
        # Same native navigation, with the observed mixed-width parenthesis
        # pair and label-before-parenthesized-link spelling. Compare syntax
        # only: retain the original written label, never alter body values.
        compared = label.translate(str.maketrans({'（': '(', '）': ')'}))
        if re.fullmatch(r'(?:\([\u3400-\u9fff]{2,8}[：:]?\)|'
                r'[\u3400-\u9fff]{2,8}[：:]\(\))', compared):
            return written
    # The publisher's rss_close role and links establish related navigation;
    # heading level/short label wording are not article identity. Unlike the
    # old prefix-only test, reject any extra unlinked sentence/title/numbers.
    if not re.fullmatch(r'(?:（[\u3400-\u9fff]{2,8}[：:]?）|\([\u3400-\u9fff]{2,8}[：:]?\)|'
            r'【[\u3400-\u9fff]{2,8}】|[\u3400-\u9fff]{2,8}[：:])', label):
        raise NewsError('article_identity_not_verified')
    return written


def _whole_editor(scope, body, *, version=1, related_labels=None):
    """Validate all written editor nodes before the three exact exclusions.

    Unknown content is an explicit parser gap, not permission to silently trim
    it.  Written captions are kept; image alt text is not a second caption.
    """
    for node in _children(scope):
        if node.name == 'h1':
            continue
        if node is body or node.name == 'div' and node.get('class') == ['publish']:
            continue
        raise NewsError('article_identity_not_verified')
    excluded = set()
    for node in body.select('.ad-detail, .rss_close, .news-foot'):
        if any(id(parent) in excluded for parent in node.parents):
            raise NewsError('article_identity_not_verified')
        classes = set(node.get('class', []))
        _visible(node)
        if 'ad-detail' in classes:
            if (node.parent is not body or node.name != 'div' or node.get_text(strip=True)
                    or any((child.name != 'div' and not (version >= 3 and child.name == 'script'
                            and str(child.get('type', '')).strip().lower() in {'', 'text/javascript', 'application/javascript'}))
                        or child.name != 'script' and child.get_text(strip=True) for child in node.find_all())):
                raise NewsError('article_identity_not_verified')
        elif 'rss_close' in classes:
            if version >= 3:
                label = _related_label(node, version=version)
                if related_labels is not None:
                    related_labels.append(label)
            else:
                value = node.get_text('', strip=True)
                if (node.name not in {'div', 'h3'} or not node.select('a[href]')
                        or not value.startswith(('（延伸閱讀：', '（熱門點閱：')) or not value.endswith('）')
                        or any(child.name not in {'h2', 'h3', 'strong', 'a', 'span', 'br'} for child in node.find_all())):
                    raise NewsError('article_identity_not_verified')
        else:
            labels = _one(node, ':scope > .news-label')
            if (node.parent is not body or node.name != 'div' or list(_children(node)) != [labels]
                    or any(child.name not in {'a', 'span'} for child in labels.find_all())
                    or any(link.get('href') != '/search/' + link.get_text(strip=True) for link in labels.select('a'))):
                raise NewsError('article_identity_not_verified')
        excluded.add(id(node))
    captions = []
    for node in body.find_all():
        if id(node) in excluded or any(id(parent) in excluded for parent in node.parents):
            continue
        if node.name not in _PROSE_TAGS:
            raise NewsError('article_identity_not_verified')
        _visible(node)
        if node.get('class') == ['mbt-text']:
            captions.append(node.get_text(' ', strip=True))
    return captions


def _desk_navigation_profile(href):
    """A relative profile navigation only, not proof of the desk's identity.

    The observed house desk's visible/schema/head credit agrees while its
    literal navigation path does not. Never guess/correct that path or request
    it. Named people, external/encoded/query/fragment routes cannot use this.
    """
    if (not isinstance(href, str) or not 0 < len(href) <= 256 or not href.startswith('/author/')
            or any(ord(c) < 32 or c in '\\%?#' for c in href) or '..' in href):
        return False
    parts = urlsplit(href)
    return bool(not parts.scheme and not parts.netloc and href == parts.path and href[len('/author/'):].strip())


def _native_byline(scope, version):
    """Two literal source layouts; no current-staff-list as historical identity.

    v1 is unchanged. v2 additionally supports the observed single author link
    around a written .author.au2 span. Its schema/visible/profile agreement is
    publisher attribution, not evidence of employment, human identity or rights.
    """
    inverse = False
    if version == 1:
        byline = _one(scope, ':scope > .publish > .publish-text > .author', 'article_attribution_not_verified')
        _visible(byline)
        links = list(_children(byline))
    else:
        publication = _one(scope, ':scope > .publish > .publish-text', 'article_attribution_not_verified')
        _visible(publication)
        wrappers = publication.select(':scope > .author')
        singles = publication.select(':scope > a > span.author' if version >= 3 else ':scope > a > span.author.au2')
        if len(wrappers) + len(singles) != 1:
            raise NewsError('article_attribution_not_verified')
        if wrappers:
            byline = wrappers[0]
            links = list(_children(byline))
        else:
            inverse = True
            byline = singles[0]
            link = byline.parent
            allowed_classes = (['author'], ['author', 'au2']) if version >= 3 else (['author', 'au2'],)
            if (byline.name != 'span' or byline.get('class') not in allowed_classes
                    or list(_children(link)) != [byline] or byline.find_all(recursive=False)):
                # The observed span holds literal text, not nested markup.
                raise NewsError('article_attribution_not_verified')
            links = [link]
    _visible(byline)
    names = [link.get_text(' ', strip=True) for link in links]
    def profile_agrees(link, name):
        href = str(link.get('href', '')).strip(' \t\r\n') if version >= 3 else link.get('href')
        return href == '/author/' + name or (version == 4 and inverse and names == ['上報快訊']
            and _desk_navigation_profile(href))

    if (not 1 <= len(links) <= 2 or any(link.name != 'a' or not name or not profile_agrees(link, name)
            for link, name in zip(links, names))):
        raise NewsError('article_attribution_not_verified')
    for link in links:
        _visible(link)
    house = len(names) == 2 and names[0] == '上報快訊' and re.fullmatch(r'[\u3400-\u9fff·]{2,12}', names[1])
    staff = len(names) == 1 and names[0] in _REVIEWED_EDITORIAL_STAFF
    desk = inverse and names == ['上報快訊']
    named = inverse and len(names) == 1 and re.fullmatch(r'[\u3400-\u9fff·]{2,12}', names[0])
    if not (house or staff or desk or named):
        raise NewsError('article_attribution_not_verified')
    if version >= 2 and any(re.search(r'中央社|路透|美聯社|法新社|新華社|彭博|聯合報|工商時報|自由時報', name) for name in names):
        raise NewsError('article_rights_or_scope_excluded')
    role = ('publisher_branded_desk_and_named_editor' if house else
        'current_about_declared_editorial_staff' if staff else
        'publisher_branded_desk' if desk else 'publisher_declared_named_byline')
    return byline, names, role, inverse


def _original_json_scripts(payload):
    """Bounded syntax-only read of original UTF-8 scripts, before HTML CR folding.

    This fallback neither executes JavaScript nor interprets article prose. It
    is used only after a strict DOM JSON failure; the caller binds every script
    to its DOM counterpart before attempting the existing LF-only repair.
    """
    if not isinstance(payload, bytes) or not 0 < len(payload) <= 2 << 20:
        raise ValueError('bounded_original_HTML_required')

    class Scripts(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=False)
            self.values, self.parts, self.size = [], None, 0

        def handle_starttag(self, tag, attrs):
            if tag != 'script':
                return
            types = [value for name, value in attrs if name == 'type']
            if 'application/ld+json' in types:
                if types != ['application/ld+json'] or len(self.values) >= 64:
                    raise ValueError('ambiguous_or_excessive_original_scripts')
                self.parts, self.size = [], 0

        def handle_startendtag(self, tag, attrs):
            if tag == 'script' and ('type', 'application/ld+json') in attrs:
                raise ValueError('ambiguous_self_closing_script')

        def handle_data(self, data):
            if self.parts is not None:
                self.size += len(data)
                if self.size > 262144:
                    raise ValueError('original_script_too_large')
                self.parts.append(data)

        def handle_endtag(self, tag):
            if tag == 'script' and self.parts is not None:
                self.values.append(''.join(self.parts))
                self.parts = None

    parser = Scripts()
    parser.feed(payload.decode('utf-8', errors='strict')); parser.close()
    if parser.parts is not None:
        raise ValueError('unclosed_original_script')
    return parser.values


def upmedia_public_modern_article(soup, *, page_url, adapter_contract_version=1, raw_payload=None):
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3, 4}:
        raise ValueError('unsupported_upmedia_article_contract')
    if (adapter_contract_version >= 3 and isinstance(raw_payload, bytes)
            and re.search(br'[\x00-\x08\x0b\x0c\x0e-\x1f]', raw_payload)):
        # Do not accept a spelling or JSON value after HTML silently removed a
        # literal C0 control. Original TAB/CR/LF remain available to the decoder.
        raise NewsError('article_identity_not_verified')
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'www.upmedia.mg' or parts.query or parts.fragment
            or not re.fullmatch(UPMEDIA_PATH_PATTERN, parts.path)
            or _one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'):
        raise NewsError('article_identity_not_verified')
    articles, syntax_notices, original_scripts = [], [], None
    scripts = soup.select('script[type="application/ld+json"]')
    for index, script in enumerate(scripts):
        try:
            raw = script.get_text()
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError as error:
                if adapter_contract_version < 3 or len(raw) > 262144:
                    raise
                # Reuse the existing strict, bounded, value-preserving root
                # NewsArticle.description LF decoder, not a new permissive JSON
                # loader or source/authorization path. Other fields still fail.
                from downloader.news_fulltext_videoland import _description_lf_repair
                if original_scripts is None:
                    original_scripts = _original_json_scripts(raw_payload)
                    if len(original_scripts) != len(scripts) or any(
                            original.replace('\r\n', '\n').replace('\r', '\n') != node.get_text()
                            for original, node in zip(original_scripts, scripts)):
                        raise ValueError('original_and_DOM_scripts_do_not_correspond')
                original = original_scripts[index]
                try:
                    json.loads(original)
                except json.JSONDecodeError as original_error:
                    decoded, proof = _description_lf_repair(original, original_error)
                else:
                    raise ValueError('original_strict_JSON_does_not_need_repair')
                if decoded is None:
                    raise error
                proof.update(original_UTF8_HTML_script_verified=True,
                    original_script_sha256=hashlib.sha256(original.encode('utf-8')).hexdigest(),
                    DOM_line_endings_may_be_normalized=True)
                syntax_notices.append(proof)
            articles.extend(obj for obj in _objects(decoded) if obj.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared = articles[0]
    if 'isAccessibleForFree' in declared and declared['isAccessibleForFree'] is not True:
        raise NewsError('paywall_or_login')
    publisher = declared.get('publisher')
    if (declared.get('url') != page_url
            or declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
            or publisher.get('name') != '上報' or publisher.get('url') != 'https://www.upmedia.mg'):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'div.news-box')
    heading, body = _one(scope, ':scope > h1'), _one(scope, UPMEDIA_BODY_SELECTOR)
    for node in (scope, heading, body):
        _visible(node)
    title = heading.get_text(' ', strip=True)
    if (not title or declared.get('headline') != title + ' -- 上報'
            or _one(soup, 'head > meta[property="og:title"]').get('content') != declared['headline']):
        raise NewsError('article_identity_not_verified')
    related_labels = []
    captions = _whole_editor(scope, body, version=adapter_contract_version, related_labels=related_labels)
    byline, names, role, inverse = _native_byline(scope, adapter_contract_version)
    raw_name = '／'.join(names)
    author, profile = declared.get('author'), 'https://www.upmedia.mg/author/' + raw_name
    if (not isinstance(author, dict) or author.get('@type') != 'Person' or author.get('name') != raw_name
            or author.get('url') != profile
            or _one(soup, 'head > meta[property="article:author"]').get('content') != profile):
        raise NewsError('article_attribution_not_verified')
    # Search written source material only. A link/sidebar mentioning a wire
    # service is not a reprint credit, and a photo credit is not its news text.
    from downloader.news_fulltext_policy import _plain_text_node
    prose = _plain_text_node(body, UPMEDIA_EXCLUSIONS, parser='lxml')
    if _REPRINT.search(prose):
        raise NewsError('article_rights_or_scope_excluded')
    visible_clock = _one(scope, ':scope > .publish > .publish-text > .pub-date')
    _visible(visible_clock)
    visible_raw = visible_clock.get_text(' ', strip=True)
    published_raw, modified_raw = declared.get('datePublished'), declared.get('dateModified')
    published = _clock(published_raw)
    if published.strftime('%Y年%m月%d日 %H:%M:%S') != visible_raw:
        raise NewsError('invalid_bulk_publication_clock')
    if modified_raw is not None and _clock(modified_raw) < published:
        raise NewsError('invalid_bulk_publication_clock')
    detail = {'contract_version': adapter_contract_version, 'source_visible_byline_raw': byline.get_text(' ', strip=True),
        'source_visible_credit_names': names, 'source_schema_author_name_raw': raw_name,
        'source_schema_author_type_raw': author['@type'], 'source_schema_profile_url_raw': profile,
        'source_credit_role': role,
        'original_ownership_verified': False, 'human_identity_verified': False,
        'historical_employment_verified': False}
    if adapter_contract_version >= 2:
        detail.update(source_visible_byline_layout='anchor_contains_written_author_span' if inverse else 'written_author_span_contains_anchor',
            publisher_attribution_is_not_an_ownership_or_employment_witness=True)
    if adapter_contract_version >= 3:
        detail.update(source_visible_author_profile_hrefs_DOM=[n.get('href') for n in scope.select(UPMEDIA_AUTHOR_SELECTOR_V3)],
            profile_attribute_representation='HTML_DOM; original byte spelling retained in raw response',
            profile_comparison_only_outer_ASCII_whitespace=True, profile_percent_decoding_used=False)
    if adapter_contract_version == 4:
        matches = [str(n.get('href', '')).strip(' \t\r\n') == '/author/' + name
            for n, name in zip(scope.select(UPMEDIA_AUTHOR_SELECTOR_V3), names)]
        detail.update(visible_profile_matches_written_credit=matches,
            house_desk_nonmatching_profile_navigation=not all(matches),
            profile_link_is_not_an_author_identity_witness=True,
            source_profile_navigation_corrected_or_requested=False)
    notices = {'contract_version': 1, 'visible_publication_clock_raw': visible_raw,
        'schema_publication_clock_raw': published_raw, 'schema_modified_clock_raw': modified_raw,
        'source_declared_timezone_offset_raw': '+08:00', 'source_declared_clock_format': 'second',
        'effective_clock_precision_verified': False, 'historical_first_release_verified': False,
        'exact_PIT_eligible': False, 'media_or_scripts_requested_or_executed': False,
        'written_image_captions': captions, 'AI_tools_and_related_articles_are_body': False}
    if adapter_contract_version >= 3:
        notices.update(source_related_navigation_labels_raw=related_labels,
            source_description_LF_syntax_notices=syntax_notices,
            native_ad_slot_scripts_are_excluded_without_execution=True)
    return {'headline': title, 'author': [{'name': raw_name}], 'datePublished': published_raw,
        'dateModified': modified_raw,
        'published_time_basis': 'current_source_schema_second_format_+08:00_corresponds_visible_clock; historical_first_release_and_effective_precision_unverified',
        'article_identity_basis': 'literal_modern_URL_canonical_og_NewsArticle_publisher_heading_visible_byline_clock_and_whole_editor_v' + str(adapter_contract_version),
        'author_attribution_detail_json': json.dumps(detail, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': 'whole_visible_native_editor_prose_tables_and_written_captions_once; exact_ads_related_tags_AI_tools_excluded; images_not_transcribed'}, scope
