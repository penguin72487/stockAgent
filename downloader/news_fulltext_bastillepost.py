"""Bastillepost's observed public native article, without HTTP or discovery.

The schema's corporate ``Person`` label is preserved as a declaration, not
promoted to a verified human, original authorship, ownership or historic PIT.
Only actual same-ID redirects and matching current declarations bind a body.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
from itertools import islice
import json
import re
from urllib.parse import unquote, urljoin, urlsplit

from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS, NewsError, _objects, normalize_url
from downloader.news_fulltext_public_originals import _one, _visible


BASTILLE_ORIGIN = 'https://www.bastillepost.com'
BASTILLE_PATH_PATTERN = r'^/hongkong/article/[1-9][0-9]*(?:-[^/?#]+)?/?$'
BASTILLE_BODY_SELECTOR = ':scope > .bastillepost-native-body'
BASTILLE_TITLE_SELECTOR = '.news-large__title-main > h2.h2.bpcategory-special-color'
_CREDIT = {'@type': 'Person', 'name': '巴士的報'}
_TAGS = frozenset({'p', 'br', 'a', 'img', 'span', 'strong', 'b', 'em', 'i', 'u',
    'div', 'h2', 'h3', 'h4', 'ul', 'ol', 'li', 'blockquote', 'table', 'caption',
    'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'figure', 'figcaption'})
_HIDDEN = frozenset({'hidden', 'd-none', 'sr-only', 'screen-reader-text', 'visually-hidden'})
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00')


def bastillepost_article_id(url, *, requested=False, adapter_contract_version=1):
    """Recognize literal URLs only; never generate a guessed slug or alias."""
    if (not isinstance(url, str) or type(requested) is not bool
            or type(adapter_contract_version) is not int or adapter_contract_version not in BASTILLE_LAYOUT_VERSIONS.values()):
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme not in ({'http', 'https'} if requested else {'https'})
            or parts.netloc != 'www.bastillepost.com' or parts.fragment
            or parts.query and not (adapter_contract_version >= 2 and parts.query == 'current_cat=5'
                or adapter_contract_version >= 4 and re.fullmatch(r'current_cat=[1-9][0-9]{0,11}', parts.query))
            or not re.fullmatch(BASTILLE_PATH_PATTERN, parts.path)):
        return None
    match = re.fullmatch(r'/hongkong/article/([1-9][0-9]*)(?:-([^/?#]+))?/?', parts.path)
    slug = match[2] or ''
    # Percent case, a source-declared slug and a trailing slash do not change
    # the explicit numeric ID. Encoded routes/queries or malformed escapes do.
    if (re.search(r'%(?![0-9a-fA-F]{2})', slug)
            or any(c in unquote(slug) for c in '/?#\\')
            or any(ord(c) < 33 or ord(c) == 127 for c in url)):
        return None
    return match[1]


def bastillepost_transport_verified(response, request_url, *, adapter_contract_version=1):
    """Require the actual chain; matching final IDs alone are insufficient."""
    def identity(url, *, requested=False):
        return bastillepost_article_id(url, requested=requested, adapter_contract_version=adapter_contract_version)

    article_id = identity(request_url, requested=True)
    if article_id is None or identity(response.url) != article_id:
        return False
    hops = getattr(response, 'redirects', ())
    if not isinstance(hops, tuple) or len(hops) > 5:
        return False
    previous = normalize_url(request_url)
    visited = {previous}
    for hop in hops:
        if (hop.from_url != previous or hop.status not in {301, 302, 303, 307, 308}
                or identity(hop.from_url, requested=True) != article_id
                or identity(hop.to_url, requested=True) != article_id
                or urlsplit(previous).scheme == 'https' and urlsplit(hop.to_url).scheme != 'https'
                or not isinstance(hop.location, str) or not hop.location
                or not isinstance(hop.response_sha256, str)
                or not re.fullmatch(r'[a-f0-9]{64}', hop.response_sha256)
                or type(hop.response_bytes) is not int or hop.response_bytes < 0
                or not isinstance(hop.observed_at_utc, str)):
            return False
        try:
            clock = datetime.fromisoformat(hop.observed_at_utc)
            if (clock.tzinfo is None or normalize_url(urljoin(previous, hop.location)) != hop.to_url
                    or hop.to_url in visited):
                return False
        except (ValueError, NewsError):
            return False
        previous = hop.to_url
        visited.add(previous)
    return previous == response.url


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if getattr(parent, 'attrs', None) and _HIDDEN.intersection(parent.get('class', [])):
            raise NewsError('article_identity_not_verified')


def _own_visible(node):
    """Ancestors of the bounded native tree have already been checked once."""
    style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
    if (node.has_attr('hidden') or str(node.get('aria-hidden', '')).lower() == 'true'
            or 'display:none' in style or 'visibility:hidden' in style
            or _HIDDEN.intersection(node.get('class', []))):
        raise NewsError('article_identity_not_verified')


def bastillepost_validation_metadata(soup, *, page_url, adapter_contract_version=1):
    """Bounded failure evidence from the existing DOM; no prose or requests.

    This observes unknown signatures, clocks and body shapes without accepting
    them, broadening scope, mutating the DOM or re-fetching an old question.
    """
    def value(text):
        if not isinstance(text, str):
            return {'type': type(text).__name__}
        return {'value': text[:512], 'characters': len(text),
                'sha256': hashlib.sha256(text.encode()).hexdigest(), 'truncated': len(text) > 512}

    def values(selector, attr=None):
        nodes = soup.select(selector)
        return {'node_count': len(nodes), 'truncated': len(nodes) > 8,
            'values': [value(n.get(attr) if attr else n.get_text(' ', strip=True)) for n in nodes[:8]]}

    bodies = soup.select('main#main .article-page__column.__main > .article-body')
    facts = {'contract_version': 1, 'literal_url_recognized': bastillepost_article_id(
        page_url, adapter_contract_version=adapter_contract_version) is not None,
        'native_scope_count': len(soup.select('main#main .article-page__column.__main')),
        'native_body_count': len(bodies), 'native_body_shapes': [],
        'titles': values('main#main .news-large ' + BASTILLE_TITLE_SELECTOR),
        'visible_clocks': values('main#main .news-large .news-large__title-main > span.news-large__data.date'),
        'published': values('head meta[property="article:published_time"]', 'content'),
        'modified': values('head meta[property="article:modified_time"]', 'content'),
        'body_prose_retained': False, 'raw_response_retained': False,
        'article_identity_or_scope_granted': False, 'additional_requests': 0}
    try:
        obj = _article_schema(soup)
        author, publisher = obj.get('author'), obj.get('publisher')
        facts['schema'] = {key: value(obj.get(key)) for key in ('@type', 'headline', 'datePublished', 'dateModified')}
        facts['schema']['author'] = ({key: value(author.get(key)) for key in ('@type', 'name')}
                                      if isinstance(author, dict) else {'type': type(author).__name__})
        facts['schema']['publisher'] = ({key: value(publisher.get(key)) for key in ('@type', 'name', 'url')}
                                         if isinstance(publisher, dict) else {'type': type(publisher).__name__})
        facts['schema']['articleBody_present'] = 'articleBody' in obj
    except NewsError as error:
        facts['schema_error'] = error.code
    for body in bodies[:2]:
        nodes = body.find_all(True, limit=4097)
        counts = {}; invisible = 0; unknown = set()
        for node in nodes[:4096]:
            counts[node.name] = counts.get(node.name, 0) + 1
            if node.name not in _TAGS:
                unknown.add(node.name)
            try:
                _own_visible(node)
            except NewsError:
                invisible += 1
        facts['native_body_shapes'].append({'elements_sampled': min(4096,len(nodes)),
            'elements_truncated': len(nodes)>4096, 'tag_counts': dict(sorted(counts.items())[:64]),
            'unknown_tags': sorted(unknown)[:32], 'invisible_nodes_sampled': invisible,
            'direct_child_tags': [n.name for n in body.find_all(recursive=False,limit=32)],
            'direct_text_characters': [len(str(n).strip()) for n in body.find_all(string=True,recursive=False,limit=32)]})
    return facts


BASTILLE_WRITTEN_BOUNDARY_QUESTION = 'bastillepost_written_body_boundary_v1'
BASTILLE_ACTIVE_RESOURCE_QUESTION = 'bastillepost_active_resource_and_body_relationship_v1'
BASTILLE_RESIDENT_EVIDENCE_VERSIONS = frozenset({8, 9, 10, 11})


def _boundary_signature(node):
    classes = node.get('class', [])
    return {'tag': node.name[:64], 'classes': [c for c in classes[:8]
        if isinstance(c, str) and re.fullmatch(r'[A-Za-z0-9_:-]{1,64}', c)],
        'classes_truncated': len(classes) > 8}


def bastillepost_written_boundary_metadata(soup, *, page_url):
    """One fixed structural question; no body, names, scripts or media retained.

    A missing direct container and an unsafe child are different failures.
    Observe both in one bounded DOM pass, without retrying the current parser
    as its own candidate or treating component counts as identity proof.
    """
    signature = _boundary_signature

    scopes = soup.select('main#main .article-page__column.__main')
    result = {'contract_version': 1, 'question': BASTILLE_WRITTEN_BOUNDARY_QUESTION,
        'literal_url_recognized': bastillepost_article_id(page_url, adapter_contract_version=2) is not None,
        'native_scope_count': len(scopes), 'scopes': [],
        'body_or_names_or_script_values_exported': False,
        'article_identity_or_scope_granted': False, 'current_parser_replayed': False,
        'additional_requests': 0}
    for scope in scopes[:2]:
        nodes = scope.find_all(True, limit=2049)
        sampled = nodes[:2048]
        bodies = [n for n in sampled if 'article-body' in n.get('class', [])]
        entry = {'elements_sampled': len(sampled), 'elements_truncated': len(nodes) > 2048,
            'direct_children': [signature(n) for n in scope.find_all(recursive=False, limit=32)],
            'body_candidates_sampled': len(bodies), 'bodies': []}
        for body in bodies[:4]:
            body_nodes = body.find_all(True, limit=2049)
            counts = {}; active = []
            for node in body_nodes[:2048]:
                tag = node.name if re.fullmatch(r'[a-z0-9:-]{1,64}', node.name) else 'other'
                counts[tag] = counts.get(tag, 0) + 1
                if node.name in {'script', 'iframe', 'object', 'embed'} and len(active) < 8:
                    raw = node.get_text()
                    src = node.get('src')
                    try:
                        parts = urlsplit(src) if isinstance(src, str) else None
                        origin = (parts.scheme + '://' + parts.hostname if parts
                            and parts.scheme in {'http', 'https'} and parts.hostname
                            and len(parts.hostname) <= 255 else None)
                    except ValueError:
                        origin = None
                    active.append({**signature(node), 'parent': signature(node.parent),
                        'inline_characters': len(raw), 'inline_sha256': hashlib.sha256(raw.encode()).hexdigest(),
                        'external_origin': origin, 'src_value_retained': False,
                        'has_event_attributes': any(str(k).lower().startswith('on') for k in node.attrs)})
            paragraphs = body.find_all('p', limit=17)
            entry['bodies'].append({'direct_native_child': body.parent is scope,
                'parent': signature(body.parent), 'text_characters': len(body.get_text('\n', strip=True)),
                'direct_children': [signature(n) for n in body.find_all(recursive=False, limit=32)],
                'tag_counts': dict(sorted(counts.items())[:64]), 'elements_truncated': len(body_nodes) > 2048,
                'paragraphs_sampled': [{**signature(p), 'direct_child': p.parent is body,
                    'characters': len(p.get_text('\n', strip=True)),
                    'sentence_ending': p.get_text('\n', strip=True).endswith(('。', '！', '？'))}
                    for p in paragraphs[:16]], 'paragraphs_truncated': len(paragraphs) > 16,
                'active_nodes': active, 'active_nodes_truncated': sum(
                    counts.get(tag, 0) for tag in ('script', 'iframe', 'object', 'embed')) > 8})
        result['scopes'].append(entry)
    return result


def bastillepost_active_resource_metadata(soup, *, page_url):
    """A NEW bounded question: resource grammar and sibling relationships.

    Do not repeat the prior counts-only question, execute code, fetch src,
    export query values/inline code/prose or turn a resource into body proof.
    Relative URLs are resolved only as lexical metadata, never request targets.
    """
    def src_profile(node):
        raw = node.get('src')
        value = {'src_attribute_present': node.has_attr('src'), 'src_form':'missing',
            'src_characters':len(raw) if isinstance(raw,str) else None,
            'src_sha256':hashlib.sha256(raw.encode()).hexdigest() if isinstance(raw,str) else None,
            'resolved_origin':None, 'path_raw':None, 'query_keys':[],
            'query_values_retained':False, 'resource_requested':False}
        if raw is None:
            return value
        if not isinstance(raw,str) or len(raw)>4096 or re.search(r'[\x00-\x20\x7f]',raw):
            value['src_form'] = 'invalid_or_over_bound'
            return value
        if not raw:
            value['src_form'] = 'empty'
            return value
        try:
            original = urlsplit(raw)
            parts = urlsplit(urljoin(page_url,raw))
            if (parts.scheme not in {'https','http'} or not parts.hostname or parts.username is not None
                    or parts.password is not None or len(parts.hostname)>255
                    or not re.fullmatch(r'[A-Za-z0-9.-]+',parts.hostname)):
                raise ValueError('unreviewed resource URI')
            port = parts.port
            value.update(src_form=('absolute' if original.scheme else 'protocol_relative'
                if raw.startswith('//') else 'root_relative' if raw.startswith('/') else 'relative'),
                resolved_origin=parts.scheme+'://'+parts.hostname+((':'+str(port)) if port else ''),
                path_raw=parts.path if re.fullmatch(r'[A-Za-z0-9_./%~-]{0,192}',parts.path) else None,
                path_characters=len(parts.path), path_sha256=hashlib.sha256(parts.path.encode()).hexdigest(),
                query_present=bool(parts.query), fragment_present=bool(parts.fragment))
            pairs = parts.query.split('&',16) if parts.query else []
            safe_keys = {'id','article_id','v','ver','version','async','lang','src','autoplay',
                'size','width','height','ref','type','channel','data','token'}
            value['query_keys'] = sorted({p.partition('=')[0] for p in pairs[:16]
                if p.partition('=')[0] in safe_keys})
            value['unrecognized_query_key_count_sampled'] = sum(p.partition('=')[0] not in safe_keys for p in pairs[:16])
            value['query_keys_truncated'] = len(pairs)>16
        except ValueError:
            value['src_form'] = 'unsupported_or_invalid'
        return value

    def context(node):
        text = node.get_text(' ',strip=True)
        return {**_boundary_signature(node), 'text_characters':len(text),
            'text_sha256':hashlib.sha256(text.encode()).hexdigest(),
            'known_relationship_label':text if text in {'往下看更多文章','相關文章','閱讀更多'} else None}

    scopes = soup.select('main#main .article-page__column.__main')
    result = {'contract_version':1, 'question':BASTILLE_ACTIVE_RESOURCE_QUESTION,
        'native_scope_count':len(scopes), 'scopes':[], 'additional_requests':0,
        'raw_or_names_or_body_retained':False, 'inline_code_or_query_values_exported':False,
        'current_parser_replayed':False, 'identity_or_scope_granted':False,
        'resolved_resource_urls_are_lexical_metadata_only':True}
    body_budget, active_budget = 4, 16
    for scope in scopes[:2]:
        nodes = scope.find_all(True,limit=4097)
        bodies = [n for n in nodes[:4096] if 'article-body' in n.get('class',[])]
        entry = {'elements_truncated':len(nodes)>4096,'body_candidates_sampled':len(bodies),'bodies':[]}
        for index, body in enumerate(bodies[:body_budget]):
            body_budget -= 1
            children = body.find_all(True,limit=4097)
            active = [n for n in children[:4096] if n.name in {'script','iframe','object','embed'}]
            detail = {'index_in_native_scope':index,'direct_native_child':body.parent is scope,
                'parent':_boundary_signature(body.parent),'elements_truncated':len(children)>4096,
                'direct_children':[_boundary_signature(n) for n in body.find_all(recursive=False,limit=16)],
                'previous_siblings':[context(n) for n in body.find_previous_siblings(limit=2)],
                'next_siblings':[context(n) for n in body.find_next_siblings(limit=2)],
                'active_count_sampled':len(active),'active_nodes':[],
                'active_nodes_truncated':len(active)>min(8,active_budget)}
            for node in active[:min(8,active_budget)]:
                active_budget -= 1
                raw = node.get_text()
                raw_type = node.get('type')
                mime = raw_type if isinstance(raw_type,str) and re.fullmatch(r'[A-Za-z0-9/+.~-]{1,80}',raw_type) else None
                detail['active_nodes'].append({**_boundary_signature(node), 'parent':_boundary_signature(node.parent),
                    'type_present':node.has_attr('type'),'type_raw':mime,
                    'type_sha256':hashlib.sha256(str(raw_type).encode()).hexdigest() if raw_type is not None else None,
                    'attribute_names':sorted(k for k in node.attrs if re.fullmatch(r'[A-Za-z0-9_-]{1,64}',k))[:16],
                    'has_event_attributes':any(str(k).lower().startswith('on') for k in node.attrs),
                    'inline_characters':len(raw),'inline_sha256':hashlib.sha256(raw.encode()).hexdigest(),
                    'ancestor_chain':[_boundary_signature(n) for n in islice(node.parents,3)],
                    'src':src_profile(node)})
            entry['bodies'].append(detail)
        entry['body_output_truncated'] = len(bodies)>len(entry['bodies'])
        result['scopes'].append(entry)
    return result


def _article_schema(soup):
    scripts = soup.select('head script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 8:
        raise NewsError('article_identity_not_verified')
    articles = []
    for script in scripts:
        raw = script.get_text()
        if len(raw) > 262144:
            raise NewsError('article_identity_not_verified')
        try:
            data = json.loads(raw)
            for index, obj in enumerate(_objects(data)):
                if index >= 128:
                    raise NewsError('article_identity_not_verified')
                if obj.get('@type') == 'NewsArticle':
                    articles.append(obj)
        except (ValueError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    return articles[0]


def _complete_short_native_primary_prose(native, title, *, adapter_contract_version=2):
    """Bounded complete primary prose, not a caption/summary-length shortcut.

    Called only after the whole native tree, title, identity, credit and clocks
    have passed. The observed short financial bulletin has one primary prose
    paragraph and an optional unfetched image. v4 additionally recognizes two
    direct primary paragraphs, both complete sentences, with no other content.
    """
    paragraphs = native.select(':scope > p.font-size-scaler-control:not(.caption)')
    if adapter_contract_version >= 4 and len(paragraphs) == 2:
        texts = [p.get_text('\n', strip=True) for p in paragraphs]
        if (not 0 < len(native.get_text('\n', strip=True)) < 100
                or any(not text or text == title or not text.endswith(('。', '！', '？')) for text in texts)):
            return False
        return all(any(node is p for p in paragraphs) if getattr(node, 'name', None) is not None
            else not str(node).strip() for node in native.contents)
    if len(paragraphs) != 1:
        return False
    paragraph = paragraphs[0]
    text = paragraph.get_text('\n', strip=True)
    # Exact private originals independently establish these six opaque native
    # suffix spellings. Preserve them as text, never infer an author/ownership.
    # Unknown annotations, ellipses and arbitrary parentheses stay unresolved.
    ending = text.endswith(('。', '！', '？')) or (adapter_contract_version >= 3
        and re.search(r'\((?:ky|sl\.?|jl|ms\.|jh)\)$', text) is not None)
    if not 0 < len(text) < 100 or text == title or not ending:
        return False
    for node in native.contents:
        if getattr(node, 'name', None) is None:
            if str(node).strip():
                return False
        elif node is not paragraph:
            if (node.name != 'a' or set(node.get('class', [])) != {'image-lightbox'}
                    or node.get_text(strip=True) or len(node.find_all(True)) != 1
                    or node.find('img', recursive=False) is None):
                return False
    return True


def _native_instagram_loader(node, native):
    """Recognize the observed empty loader, never execute or fetch it.

    This is a non-text boundary, not an Instagram body/author/license proof.
    Only the attested literal resource and native placement are recognized;
    all static surrounding text is still verified and retained normally.
    """
    parent = node.parent
    wrapper = parent.parent if parent is not None else None
    return (node.name == 'script' and set(node.attrs) == {'async', 'src'}
        and node.get('src') == '//www.instagram.com/embed.js' and not node.contents
        and parent is not None and parent.name == 'p' and not parent.get('class', [])
        and wrapper is not None and wrapper.name == 'div'
        and set(wrapper.get('class', [])) == {'article-content-item', 'type-html'}
        and wrapper.parent is native)


def _empty_https_iframe(node, *, host, path):
    """A literal, inert resource boundary, NOT permission to request a player.

    Query values are kept opaque. They cannot change this collector's behavior:
    no iframe is executed, requested, transcribed or treated as native prose.
    """
    src = node.get('src')
    if (node.name != 'iframe' or node.contents or not isinstance(src, str)
            or not 0 < len(src) <= 1024 or any(ord(c) < 33 or ord(c) == 127 for c in src)
            or re.search(r'%(?![0-9a-fA-F]{2})', src)):
        return False
    try:
        parts = urlsplit(src)
    except ValueError:
        return False
    return (parts.scheme == 'https' and parts.netloc == host and not parts.fragment
        and re.fullmatch(path, parts.path) is not None and len(parts.query) <= 512)


def _native_youtube_player(node, native):
    parent = node.parent
    return (set(node.attrs) == {'allow', 'allowfullscreen', 'frameborder', 'src'}
        and node.get('frameborder') == '0' and node.get('allowfullscreen') == ''
        and isinstance(node.get('allow'), str) and len(node['allow']) <= 512
        and _empty_https_iframe(node, host='www.youtube.com', path=r'/embed/[A-Za-z0-9_-]{11}')
        and parent is not None and parent.name == 'div'
        and parent.get('class', []) == ['youtube-video-container'] and parent.parent is native)


def _native_audio_player(node, native):
    """Observed inert same-host audio widget; do not acquire/transcribe it."""
    return (set(node.attrs) == {'allowtransparency', 'attrmap', 'src', 'style'}
        and node.get('allowtransparency') == 'true'
        and isinstance(node.get('attrmap'), str) and len(node['attrmap']) <= 512
        and isinstance(node.get('style'), str) and len(node['style']) <= 2048
        and _empty_https_iframe(node, host='www.bastillepost.com', path=r'/hongkong/article-audio')
        and node.parent is native)


def _native_text_scope(node, *, adapter_contract_version):
    """Shared visibility/explicit-credit guard; never infer original ownership."""
    _own_visible(node)
    if (any(str(key).lower().startswith('on') for key in node.attrs)
            or node.has_attr('data-paywall') or node.has_attr('contenteditable')):
        raise NewsError('article_identity_not_verified')
    if node.name in {'p', 'div', 'blockquote'} and re.match(
            r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:]|文章來源[：:])',
            node.get_text(' ', strip=True)):
        raise NewsError('article_rights_or_scope_excluded')
    if (adapter_contract_version >= 2 and node.name == 'p'
            and re.match(r'^[（(]星島日報報道[）)]', node.get_text(' ', strip=True))):
        raise NewsError('article_rights_or_scope_excluded')


def _native_document_context(soup, *, page_url, adapter_contract_version):
    """Verify current declarations only, without selecting or accepting a body."""
    article_id = bastillepost_article_id(page_url, adapter_contract_version=adapter_contract_version)
    if article_id is None:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'main#main .article-page__column.__main')
    header = _one(scope, ':scope > .news-large')
    heading = _one(header, BASTILLE_TITLE_SELECTOR)
    title = heading.get_text(' ', strip=True)
    obj = _article_schema(soup)
    publisher = obj.get('publisher')
    if (not title or obj.get('headline') != title or obj.get('name') != title
            or _one(soup, 'head meta[property="og:title"]').get('content') != title
            or _one(soup, 'head meta[property="og:type"]').get('content') != 'website'
            or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
            or publisher.get('name') != 'Bastillepost 巴士的報'
            or publisher.get('url') != BASTILLE_ORIGIN
            or 'articleBody' in obj):
        raise NewsError('article_identity_not_verified')
    declarations = [
        _one(soup, 'head link[rel=canonical]').get('href'),
        _one(soup, 'head meta[property="og:url"]').get('content'), obj.get('mainEntityOfPage'),
    ]
    if any(bastillepost_article_id(url, adapter_contract_version=adapter_contract_version) != article_id
           for url in declarations):
        raise NewsError('article_identity_not_verified')
    if obj.get('author') != _CREDIT:
        raise NewsError('article_attribution_not_verified')
    published, modified = obj.get('datePublished'), obj.get('dateModified')
    visible = _one(header, '.news-large__title-main > span.news-large__data.date')
    visible_raw = visible.get_text(' ', strip=True)
    try:
        if (not isinstance(published, str) or not _CLOCK.fullmatch(published)
                or not isinstance(modified, str) or not _CLOCK.fullmatch(modified)):
            raise ValueError('unreviewed clock')
        release, revision = datetime.fromisoformat(published), datetime.fromisoformat(modified)
        release_label = release.strftime('%Y年%m月%d日 %H:%M')
        expected = release_label + ' 最後更新：' + revision.strftime('%H:%M')
        release_only = (adapter_contract_version >= 2 and revision <= release and visible_raw == release_label)
        if (not release_only and (revision < release or visible_raw != expected)
                or _one(soup, 'head meta[property="article:published_time"]').get('content') != published):
            raise ValueError('conflicting clocks')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    modified_meta = soup.select('head meta[property="article:modified_time"]')
    if (len(modified_meta) > 1 or modified_meta and modified_meta[0].get('content') != modified):
        raise NewsError('invalid_bulk_publication_clock')
    for node in (scope, header, heading, visible):
        _native_visible(node)
    for label in (heading, visible):
        children = label.find_all(True, limit=65)
        if len(children) > 64:
            raise NewsError('article_identity_not_verified')
        for child in children:
            if child.name not in {'strong', 'b', 'span', 'em', 'i'}:
                raise NewsError('article_identity_not_verified')
            _own_visible(child)
    return scope, {'title': title, 'publisher': publisher, 'published': published, 'modified': modified,
        'visible_raw': visible_raw, 'release_only': release_only, 'modified_precedes_release': revision < release}


def bastillepost_private_continuation_scope(soup, *, page_url, adapter_contract_version):
    """Positive scope for one private double-container witness, never a body.

    The unresolved relationship is intentionally NOT guessed here. Both native
    containers stay in the exact raw response, with all neighbors. No text is
    selected, combined, published, trained on or declared source-owned.
    """
    if (type(adapter_contract_version) is not int
            or adapter_contract_version not in BASTILLE_LAYOUT_VERSIONS.values()
            or adapter_contract_version < 6):
        raise NewsError('private_original_scope_not_verified')
    scope, _ = _native_document_context(soup, page_url=page_url, adapter_contract_version=adapter_contract_version)
    bodies = scope.select(':scope > .article-body')
    if len(bodies) != 2 or len(soup.select('.article-body')) != 2:
        raise NewsError('private_original_scope_not_verified')
    audio_count = 0
    for index, body in enumerate(bodies):
        _native_visible(body)
        nodes = body.find_all(True, limit=4097)
        if len(nodes) > 4096 or not body.get_text(' ', strip=True):
            raise NewsError('private_original_scope_not_verified')
        for node in nodes:
            _native_text_scope(node, adapter_contract_version=adapter_contract_version)
            if node.name in _TAGS:
                continue
            if (index != 0 or set(node.attrs) != {'allowtransparency', 'attrmap', 'src', 'style'}
                    or not _empty_https_iframe(node, host='www.bastillepost.com', path=r'/hongkong/article-audio')):
                raise NewsError('private_original_scope_not_verified')
            audio_count += 1
    if audio_count != 1:
        raise NewsError('private_original_scope_not_verified')


def _native_article_body(soup, scope, *, page_url, adapter_contract_version):
    """Select the original scope only across an explicit different-story boundary.

    Arbitrary duplicate bodies remain ambiguous. Neither first-body position,
    title similarity nor a later paragraph's length is an identity witness.
    The following story's prose and authorship are never acquired here.
    """
    bodies = scope.select(':scope > .article-body')
    if adapter_contract_version < 7 or len(bodies) != 2:
        native = _one(scope, ':scope > .article-body', 'body_not_found')
        if len(soup.select('.article-body')) != 1:
            raise NewsError('article_identity_not_verified')
        return native, None
    if len(soup.select('.article-body')) != 2:
        raise NewsError('article_identity_not_verified')
    children = scope.find_all(True, recursive=False, limit=257)
    if len(children) > 256:
        raise NewsError('article_identity_not_verified')
    markers = [node for node in scope.select(':scope > div > h3')
        if node.get_text(' ', strip=True) == '往下看更多文章'
        and not any(node.parent is body for body in bodies)]
    if len(markers) != 1:
        raise NewsError('article_identity_not_verified')
    label, wrapper = markers[0], markers[0].parent
    parts = wrapper.find_all(True, recursive=False)
    if (len(parts) != 3 or parts[0] is not label or [n.name for n in parts] != ['h3', 'a', 'span']
            or label.find(True) is not None or set(parts[1].attrs) != {'href'}
            or set(parts[2].get('class', [])) != {'news-large__data'} or parts[2].find(True) is not None
            or parts[2].get_text(strip=True)
            or any(str(text).strip() for text in wrapper.find_all(string=True, recursive=False))):
        raise NewsError('article_identity_not_verified')
    anchor = parts[1]
    heading = anchor.find_all(True, recursive=False)
    if (len(heading) != 1 or heading[0].name != 'h3' or heading[0].find(True) is not None
            or set(heading[0].get('class', [])) != {'bpcategory-special-color', 'read-more-post'}
            or not heading[0].get_text(strip=True)
            or anchor.get_text(' ', strip=True) != heading[0].get_text(' ', strip=True)):
        raise NewsError('article_identity_not_verified')
    for node in [wrapper, *wrapper.find_all(True, limit=65)]:
        if node.name not in {'div', 'h3', 'a', 'span'}:
            raise NewsError('article_identity_not_verified')
        _native_text_scope(node, adapter_contract_version=adapter_contract_version)
    _native_visible(wrapper)
    raw_link = anchor.get('href')
    if not isinstance(raw_link, str):
        raise NewsError('article_identity_not_verified')
    following_url = urljoin(page_url, raw_link)
    following_id = bastillepost_article_id(following_url, adapter_contract_version=adapter_contract_version)
    original_id = bastillepost_article_id(page_url, adapter_contract_version=adapter_contract_version)
    if following_id is None or following_id == original_id:
        raise NewsError('article_identity_not_verified')
    positions = {id(node): index for index, node in enumerate(children)}
    marker_index = positions[id(wrapper)]
    before = [body for body in bodies if positions[id(body)] < marker_index]
    after = [body for body in bodies if positions[id(body)] > marker_index]
    if len(before) != 1 or len(after) != 1:
        raise NewsError('article_identity_not_verified')
    native, following = before[0], after[0]
    _native_visible(following)
    banners = children[marker_index+1:positions[id(following)]]
    if (len(banners) != 2 or {tuple(sorted(n.get('class', []))) for n in banners}
            != {('banner-image', 'desktop-only'), ('banner-image', 'mobile-only')}):
        raise NewsError('article_identity_not_verified')
    for banner in banners:
        nodes = [banner, *banner.find_all(True, limit=33)]
        if len(nodes) > 32 or banner.get_text(strip=True):
            raise NewsError('article_identity_not_verified')
        for node in nodes:
            if node.name != 'div':
                raise NewsError('article_identity_not_verified')
            _native_text_scope(node, adapter_contract_version=adapter_contract_version)
    return native, {'kind': 'source_declared_following_story_boundary',
        'label_raw': label.get_text(' ', strip=True), 'following_link_raw': raw_link,
        'following_url_resolved': following_url, 'following_article_id': following_id,
        'different_literal_article_ID': True, 'original_native_body_before_boundary': True,
        'following_body_excluded': True, 'following_body_or_author_not_admitted': True,
        'following_story_requested': False, 'not_first_body_or_title_similarity_selection': True}


def bastillepost_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Bind a single native written body; no whole-page/summary fallback."""
    scope, context = _native_document_context(soup, page_url=page_url, adapter_contract_version=adapter_contract_version)
    title, publisher = context['title'], context['publisher']
    published, modified, visible_raw = context['published'], context['modified'], context['visible_raw']
    native, boundary = _native_article_body(soup, scope, page_url=page_url,
        adapter_contract_version=adapter_contract_version)
    _native_visible(native)
    elements = native.find_all(True, limit=4097)
    if len(elements) > 4096:
        raise NewsError('article_identity_not_verified')
    notices = [{'kind': 'publisher_release_and_revision_declarations', 'published_raw': published,
        'modified_raw': modified, 'visible_clock_raw': visible_raw,
        'visible_revision_has_date': False, 'schema_revision_date_not_inferred_from_time_of_day': True,
        'current_declaration_not_observed_historical_PIT': True}]
    if context['release_only']:
        # A CMS modification can precede a scheduled release. Preserve both
        # declarations; this is not proof of a draft, nor earlier public access.
        notices[0].update(visible_revision_label_present=False, modified_precedes_release=context['modified_precedes_release'],
            earlier_modified_promoted_to_publication=False, source_release_declaration_not_replaced=True)
    if boundary is not None:
        notices.append(boundary)
    loaders = []
    for node in elements:
        loader = ('instagram' if adapter_contract_version >= 5 and _native_instagram_loader(node, native)
            else 'youtube' if adapter_contract_version >= 6 and _native_youtube_player(node, native)
            else 'bastillepost' if adapter_contract_version >= 7 and _native_audio_player(node, native) else None)
        if node.name not in _TAGS and not loader:
            raise NewsError('article_identity_not_verified')
        _native_text_scope(node, adapter_contract_version=adapter_contract_version)
        if loader:
            loaders.append((node, loader))
            if len(loaders) > 8:
                raise NewsError('article_identity_not_verified')
    # Mutate only after the complete bounded native tree has passed the same
    # visibility/active/credit checks. Do not discard its parent or fallback
    # text: only an empty, unrequested loader has no native prose to retain.
    for node, provider in loaders:
        notices.append({'kind': 'unfetched_external_social_loader' if provider == 'instagram'
            else 'unfetched_native_audio_player' if provider == 'bastillepost'
            else 'unfetched_external_video_player', 'provider': provider,
            'src_raw': node['src'], 'inline_characters': 0,
            'resource_requested': False, 'script_executed': False,
            'static_native_fallback_text_retained': True, 'external_rendered_content_acquired': False,
            'external_embedding_not_author_or_ownership_proof': True})
        if provider == 'bastillepost':
            notices[-1].update(audio_transcription_acquired=False, native_audio_not_authorship_proof=True)
        node.decompose()
    short_primary = adapter_contract_version >= 2 and _complete_short_native_primary_prose(
        native, title, adapter_contract_version=adapter_contract_version)
    for image in list(native.select('img')):
        notices.append({'kind': 'unfetched_inline_image', 'src_raw': image.get('src'),
            'alt_raw': image.get('alt'), 'media_requested': False,
            'alt_promoted_to_visible_caption': False, 'image_ownership_inferred': False})
        image.decompose()
    # The shared generic extractor excludes whole figures. In an already
    # verified native article they can contain visible captions; flatten only
    # the semantic wrapper, never lose their text or fetch the image.
    for figure in native.select('figure'):
        figure.name = 'div'
    # All visible prose, links, table cells and captions remain in source order.
    # Images are not downloaded; surrounding captions/credit text are not cut.
    normalized = soup.new_tag('div', attrs={'class': 'bastillepost-native-body'})
    while native.contents:
        normalized.append(native.contents[0].extract())
    body_scope = soup.new_tag('div')
    body_scope.append(normalized)
    return {
        'headline': title, 'author': [dict(_CREDIT)],
        '_bastillepost_complete_short_primary_prose': short_primary,
        'attribution_status': 'source_declared_corporate_author_not_verified_person',
        'datePublished': published, 'dateModified': modified,
        'author_attribution_detail_json': json.dumps({
            'schema_author_raw': _CREDIT, 'schema_publisher_name': publisher['name'],
            'role': 'publisher_declared_corporate_label_not_verified_person_or_original_ownership',
            'native_human_byline_verified': False, 'photo_credit_is_not_text_author': True,
            'native_prose_credits_retained_without_interpreting_initials': True,
        }, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'article_identity_basis': 'literal_current_numeric_ID_matching_source_canonical_OG_mainEntity; '
            'single_native_title_matching_schema_and_OG; not_title_similarity_or_generated_alias',
        'published_time_basis': 'matching_head_and_schema_release_seconds_offset_+08:00; '
            'visible_release_minute_matches; current_source_declaration_not_observed_historical_PIT',
        'body_completeness': 'current_single_native_article_body_ordered_visible_prose_tables_and_captions_once; '
            'inline_images_unfetched; no_summary_replacement; not_original_historical_version'
            + ('; external_social_widgets_unexecuted_unfetched; static_native_fallback_text_not_external_rendered_fulltext'
               if any(provider != 'bastillepost' for _, provider in loaders) else '')
            + ('; native_audio_unrequested_unexecuted; audio_transcription_not_acquired'
               if any(provider == 'bastillepost' for _, provider in loaders) else '')
            + ('; source_declared_different_story_boundary_verified; following_story_not_joined_to_original'
               if boundary is not None else ''),
    }, body_scope
