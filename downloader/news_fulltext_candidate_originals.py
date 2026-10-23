"""Private exact failure witnesses, never article admission or a second HTTP owner.

The resident candidate lane reserves its once-only request before HTTP. A
worker can return an immutable response with a positive, source-bound permit;
only the queue owner durably writes the private witness and its receipt.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urljoin, urlsplit

from downloader.news_fulltext_policy import BASTILLE_LAYOUT_VERSIONS, NewsError, POCKET_LAYOUT_VERSIONS, _objects, public_article_document
from downloader.news_fulltext_pocket import (
    POCKET_AUTHOR, POCKET_ORIGIN, _native_visible, _unique_object, pocket_article_id,
    pocket_numeric_transport_verified, pocket_article_transport_verified,
)
from downloader.news_fulltext_public_originals import _one

MAX_ORIGINAL_BYTES = 2 << 20
QUESTION = 'pocket_private_native_original_boundary_v1'
LEGACY_QUESTION = 'pocket_private_legacy_route_original_boundary_v1'
BASTILLE_SHORT_QUESTION = 'bastillepost_private_short_native_original_v1'
BASTILLE_CONTINUATION_QUESTION = 'bastillepost_private_two_container_relationship_original_v1'
GLOBALVOICES_ORIGINAL_LAYOUT = 'globalvoices_private_native_original_v1'
GLOBALVOICES_ORIGINAL_QUESTION = 'globalvoices_registered_missing_body_original_v1'
STOCKFEEL_ORIGINAL_QUESTION = 'stockfeel_private_native_identity_credit_original_v1'
INEWS_ORIGINAL_QUESTION = 'inews_private_native_printed_credit_editor_original_v1'
INEWS_HEADER_ORIGINAL_QUESTION = 'inews_private_native_named_header_editor_original_v1'
ETTODAY_CREDIT_ORIGINAL_QUESTION = 'ettoday_private_native_leading_reporter_break_original_v1'
ETTODAY_INLINE_ORIGINAL_QUESTION = 'ettoday_private_native_inline_leading_reporter_break_original_v1'
ETTODAY_VISIBLE_ORIGINAL_QUESTION = 'ettoday_private_visible_reporter_person_clock_original_v1'
ETTODAY_IMAGE_ORIGINAL_QUESTION = 'ettoday_private_visible_reporter_after_image_person_clock_original_v1'
ETTODAY_NONEMPTY_ORIGINAL_QUESTION = 'ettoday_private_first_nonempty_reporter_person_clock_original_v1'
ETTODAY_CANONICAL_ORIGINAL_QUESTION = 'ettoday_private_visible_reporter_canonical_author_original_v1'
ORIGINAL_EVIDENCE_VERSIONS = frozenset({4, 5, 9, 11, 12, 13, 15, 16, 17, 18, 25, 26, 28, 29, 30, 31, 32, 33})


@dataclass(frozen=True)
class PendingOriginal:
    response: object
    facts: dict
    permit: dict


def _ettoday_visible_credit(first, *, allow_replaced_images=False, first_nonempty_line=False):
    """Read a literal visible first line, not an accepting HTML body grammar.

    Formatting tag names do not confer authorship. Comments are not rendered
    text; active/alternate content is not a printed byline. Bounded, non-text
    facts survive a refusal so another request need not guess the failed node.
    """
    from bs4 import Comment, NavigableString, Tag
    from downloader.news_fulltext_public_originals import _visible
    error = 'private_ettoday_credit_original_scope_not_verified'
    facts = {'verification_stage':'visible_prefix', 'prefix_nodes':0,
        'comment_nodes':0, 'tag_counts':{}, 'separator_found':False}
    if first_nonempty_line:
        facts['leading_blank_lines'] = 0

    def fail():
        exc = NewsError(error)
        exc.private_scope_facts = dict(facts)
        raise exc

    nontext = {'script','style','template','noscript','svg','math','canvas','iframe','frame',
        'object','embed','form','input','button','select','textarea','img','picture','video','audio'}
    prefix, characters, has_literal_text = [], 0, False
    nodes = iter(first.descendants)
    for node in nodes:
        facts['prefix_nodes'] += 1
        if facts['prefix_nodes'] > 128:
            fail()
        if isinstance(node, Comment):
            facts['comment_nodes'] += 1
            continue
        if type(node) is Tag:
            name = node.name if re.fullmatch(r'[a-z][a-z0-9-]{0,31}', node.name or '') else 'other'
            facts['tag_counts'][name] = facts['tag_counts'].get(name, 0)+1
            if allow_replaced_images and name == 'img':
                # A replaced image contributes no literal text. Neither its
                # alt/title/URL nor its visibility is an author's name. This
                # does not remove the image from, or accept, an article body.
                continue
            if name in nontext or name == 'other':
                fail()
            _visible(node)
            if name == 'br':
                if node.attrs or node.contents:
                    fail()
                if first_nonempty_line and not has_literal_text:
                    # A blank visual line is not an empty author declaration.
                    # Do not search past a nonempty non-reporter line.
                    facts['leading_blank_lines'] += 1
                    prefix.clear(); characters = 0
                    continue
                facts['separator_found'] = True
                break
        elif type(node) is NavigableString:
            prefix.append(str(node))
            characters += len(str(node))
            has_literal_text = has_literal_text or bool(str(node).strip())
            if characters > 160:
                fail()
        else:
            fail()
    signature = ''.join(prefix).strip()
    facts['prefix_characters'] = len(signature)
    facts['prefix_sha256'] = hashlib.sha256(signature.encode()).hexdigest()
    prose = any(type(node) is NavigableString and str(node).strip()
        and not any(parent.name in nontext for parent in node.parents) for node in nodes)
    facts['following_visible_text'] = bool(prose)
    return signature, prose, facts


def private_ettoday_credit_original_permit(response, source, requested_url, *, include_document=False,
                                          _inline_credit=False, _visible_credit=False, _replaced_images=False,
                                          _first_nonempty_line=False, _canonical_author_scope=False):
    """Exact public WWW original after a separately completed credit-shape lead.

    A plain native first line must name the reporter, followed by a direct BR
    and actual prose in that paragraph. Old questions require exact Person
    declarations. The canonical-author question reuses the current adapter's
    explicit author/publisher relationships; it never takes a reporter name
    from schema metadata. All modes corroborate identity and both clocks.
    This grants private diagnostic retention, never body admission,
    author inference from metadata, redistribution or training permission.
    """
    from bs4 import NavigableString, Tag
    from downloader.news_fulltext_ettoday import (
        ETTODAY_BODY_SELECTOR, _REPORTER, _clocks, _schemas, _visible,
        _visible_text, ettoday_literal_diagnostic_url, ettoday_resident_source,
    )
    error = 'private_ettoday_credit_original_scope_not_verified'
    if (not ettoday_resident_source(source)
            or not ettoday_literal_diagnostic_url(requested_url)
            or urlsplit(requested_url).hostname != 'www.ettoday.net'
            or response.status != 200 or response.redirects or response.url != requested_url
            or not isinstance(response.body, bytes) or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES
            or response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
                not in {'text/html', 'application/xhtml+xml'}):
        raise NewsError(error)
    document = public_article_document(response.body, source, page_url=requested_url)
    canonical = _one(document, 'head > link[rel~=canonical][itemprop=mainEntityOfPage][href]', error)
    social = _one(document, 'head > meta[property="og:url"][content]', error)
    scope = _one(document, '.subject_article', error)
    heading = _one(scope, ':scope > header > h1.title[itemprop=headline]', error)
    body = _one(scope, ETTODAY_BODY_SELECTOR, error)
    title = heading.get_text(' ', strip=True)
    if (canonical.get('href') != requested_url or social.get('content') != requested_url or not title
            or len(document.select('.story[itemprop=articleBody]')) != 1
            or document.html.get('itemtype') not in {'http://schema.org/NewsArticle', 'https://schema.org/NewsArticle'}
            or body.select('iframe,form,[data-paywall],.paywall,vip-content')):
        raise NewsError(error)
    _visible_text(heading); _visible(body)
    first = body.find('p', recursive=False)
    if first is None or any(n.get_text(' ',strip=True) for n in first.find_previous_siblings()):
        raise NewsError(error)
    _visible_text(first)
    trace = None
    if _visible_credit:
        signature, following_prose, trace = _ettoday_visible_credit(first,
            allow_replaced_images=_replaced_images,first_nonempty_line=_first_nonempty_line)
        separator_verified = trace['separator_found']
    else:
        prefix = []
        separator = None
        nodes = iter(first.descendants if _inline_credit else first.contents)
        for child in nodes:
            if type(child) is Tag and child.name == 'br':
                separator = child
                break
            if _inline_credit and type(child) is Tag and child.name in {'strong','b','span','em','i','u','a'}:
                continue
            if type(child) is not NavigableString:
                raise NewsError(error)
            prefix.append(str(child))
        signature = ''.join(prefix).strip()
        following_prose = (any(type(n) is NavigableString and str(n).strip() for n in nodes)
            if _inline_credit else separator is not None and any(
                n.get_text(' ',strip=True) if isinstance(n, Tag) else str(n).strip() for n in separator.next_siblings))
        separator_verified = separator is not None and not separator.attrs and not separator.contents
    try:
        if trace is not None:
            trace['verification_stage'] = 'printed_reporter'
        match = _REPORTER.fullmatch(signature)
        if not separator_verified or match is None or not following_prose:
            raise NewsError(error)
        names = match[1].split('、')
        if len(names) != len(set(names)):
            raise NewsError(error)
        if trace is not None:
            trace['verification_stage'] = 'source_clocks'
        published, modified, _ = _clocks(document, scope, 'www.ettoday.net', adapter_contract_version=39)
        if trace is not None:
            trace['verification_stage'] = 'same_article_author' if _canonical_author_scope else 'same_article_person'
        notices = _schemas(document, requested_url, title, published, modified, names,
            adapter_contract_version=39, require_headline_witness=True,
            native_named_authors=() if _canonical_author_scope else tuple(names))
        if trace is not None:
            trace['verification_stage'] = 'reprint_guard'
        if any(re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])',
                paragraph.get_text(' ',strip=True)) for paragraph in body.find_all('p')):
            raise NewsError(error)
    except NewsError as exc:
        if trace is not None:
            exc.private_scope_facts = trace
        raise
    mode = ('canonical' if _canonical_author_scope else 'nonempty' if _first_nonempty_line else 'image' if _replaced_images else
        'visible' if _visible_credit else 'inline' if _inline_credit else 'plain')
    question, scope_suffix = {
        'canonical':(ETTODAY_CANONICAL_ORIGINAL_QUESTION,'visible_reporter_canonical_author_clock'),
        'nonempty':(ETTODAY_NONEMPTY_ORIGINAL_QUESTION,'first_nonempty_reporter_person_clock'),
        'image':(ETTODAY_IMAGE_ORIGINAL_QUESTION,'visible_reporter_after_image_person_clock'),
        'visible':(ETTODAY_VISIBLE_ORIGINAL_QUESTION,'visible_reporter_person_clock'),
        'inline':(ETTODAY_INLINE_ORIGINAL_QUESTION,'person_corroborated_inline_leading_reporter_break'),
        'plain':(ETTODAY_CREDIT_ORIGINAL_QUESTION,'person_corroborated_leading_reporter_break'),
    }[mode]
    permit = {'contract_version':1, 'question':question,
        'provider':source.provider, 'url':requested_url, 'requested_url':requested_url,
        'document_url':response.url, 'response_sha256':hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint':source.data_fingerprint, 'authorization_fingerprint':source.authorization_fingerprint,
        'printed_signature_sha256':hashlib.sha256(signature.encode()).hexdigest(),
        'scope':'known_public_native_publisher_'+scope_suffix+'_private_evidence_only'}
    if _canonical_author_scope:
        # Only finite notice kinds are public diagnostic facts. Literal names,
        # headline and source schema values stay in the private original.
        permit['author_relationship_contract'] = 'ettoday_visible_article_v39'
        permit['author_relationship_notices'] = sorted({n['kind'] for n in notices if n['kind'] in {
            'schema_organization_author_not_reporter', 'schema_declares_last_of_two_native_reporters',
            'schema_declares_ordered_subset_of_native_reporters'}})
        permit['reporters_taken_from_whole_visible_signature'] = True
        permit['source_values_rewritten'] = False
    return (permit, document) if include_document else permit


def private_ettoday_inline_credit_original_permit(response, source, requested_url, *, include_document=False):
    """Literal visible inline credit, never a name inferred from a tag shape."""
    return private_ettoday_credit_original_permit(response,source,requested_url,
        include_document=include_document,_inline_credit=True)


def private_ettoday_visible_credit_original_permit(response, source, requested_url, *, include_document=False):
    """Private evidence only; visible names and exact Person/clocks, not tags."""
    return private_ettoday_credit_original_permit(response,source,requested_url,
        include_document=include_document,_visible_credit=True)


def private_ettoday_image_credit_original_permit(response, source, requested_url, *, include_document=False):
    """A literal reporter after a non-text image, never alt-text attribution."""
    return private_ettoday_credit_original_permit(response,source,requested_url,
        include_document=include_document,_visible_credit=True,_replaced_images=True)


def private_ettoday_nonempty_credit_original_permit(response, source, requested_url, *, include_document=False):
    """First nonempty printed line, not an arbitrary reporter found in prose."""
    return private_ettoday_credit_original_permit(response,source,requested_url,
        include_document=include_document,_visible_credit=True,_replaced_images=True,_first_nonempty_line=True)


def private_ettoday_canonical_credit_original_permit(response, source, requested_url, *, include_document=False):
    """Current canonical author relationships; literal visible reporters only."""
    return private_ettoday_credit_original_permit(response,source,requested_url,
        include_document=include_document,_visible_credit=True,_replaced_images=True,
        _first_nonempty_line=True,_canonical_author_scope=True)


def private_inews_original_permit(response, source, requested_url, *, include_document=False):
    return _private_inews_original_permit(response, source, requested_url,
        header_credit=False, include_document=include_document)


def private_inews_header_original_permit(response, source, requested_url, *, include_document=False):
    """A named native header proves private evidence scope, not body completeness."""
    return _private_inews_original_permit(response, source, requested_url,
        header_credit=True, include_document=include_document)


def _private_inews_original_permit(response, source, requested_url, *, header_credit, include_document):
    """One known public native printed-credit witness, never body admission.

    A completed shape-only question must separately precede submission. Here
    the actual response must positively identify the same native publisher and
    contain an explicit reporter/newsroom printed signature, not a guessed
    name, empty header, third-party source or a member/challenge response.
    The writer independently repeats this gate on bytes before retention.
    """
    from downloader.news_fulltext_setn import (
        INEWS_EDITOR_SELECTOR, _inews_native_identity, inews_boundary_source, inews_literal_article_url,
    )
    from downloader.news_fulltext_public_originals import _visible
    error = 'private_inews_original_scope_not_verified'
    if (not inews_boundary_source(source) or not inews_literal_article_url(requested_url)
            or response.status != 200 or response.redirects or response.url != requested_url
            or not isinstance(response.body, bytes) or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES
            or response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
                not in {'text/html', 'application/xhtml+xml'}):
        raise NewsError(error)
    document = public_article_document(response.body, source, page_url=response.url)
    scope, _, _, _ = _inews_native_identity(document, page_url=requested_url)
    editor = _one(scope, INEWS_EDITOR_SELECTOR, error)
    body = _one(editor, ':scope > div[itemprop=articleBody]', error)
    _visible(editor); _visible(body)
    if body.select('iframe,form,[data-paywall],.paywall,vip-content'):
        raise NewsError(error)
    byline = _one(editor, ':scope > .article_remark_wrap > .author_wrap', error)
    _visible(byline)
    if not header_credit and (byline.get_text(' ', strip=True) or byline.find_all()):
        raise NewsError(error)
    clock = _one(editor, ':scope > .article_remark_wrap > time.pageDate', error)
    _visible(clock)
    clock_text = clock.get_text(' ', strip=True)
    if clock.find_all() or not re.fullmatch(r'[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}', clock_text):
        raise NewsError(error)
    try:
        datetime.strptime(clock_text, '%Y/%m/%d %H:%M')
    except ValueError:
        raise NewsError(error) from None
    if header_credit:
        link = _one(byline, ':scope > a[itemprop=author]', error)
        _visible(link)
        name, signature = link.get_text(' ', strip=True), byline.get_text(' ', strip=True)
        if (byline.find_all(recursive=False) != [link] or link.find_all()
                or not re.fullmatch(r'[\u3400-\u9fff·]{2,20}', name)
                or name in {'綜合', '整理', '編輯', '記者'}
                or not re.fullmatch(r'https://www\.setn\.com/author/[a-z0-9-]+', link.get('href', ''))
                or not re.fullmatch('記者 '+re.escape(name)+r' (?:[\u3400-\u9fff]{1,20})?報導', signature)):
            raise NewsError(error)
    else:
        first = next((n for n in body.find_all('p', recursive=False, limit=17)
                      if n.get_text(' ', strip=True)), None)
        if first is None:
            raise NewsError(error)
        _visible(first)
        signature = first.get_text(' ', strip=True)
        if (len(signature) > 160 or first.find_all()
                or not re.fullmatch(r'(?:記者[\u3400-\u9fff·]{2,20}[／/][\u3400-\u9fff]{0,20}報導|'
                    r'[\u3400-\u9fff]{2,8}中心[／/](?:[\u3400-\u9fff·]{2,20}報導|綜合報導))', signature)):
            raise NewsError(error)
    if any(re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])',
            paragraph.get_text(' ', strip=True)) for paragraph in body.find_all('p')):
        raise NewsError(error)
    # Keep its role/hash only in finite metadata. Public signature text is
    # retained only inside the separately receipted private raw original.
    permit = {'contract_version': 1,
        'question': INEWS_HEADER_ORIGINAL_QUESTION if header_credit else INEWS_ORIGINAL_QUESTION,
        'provider': source.provider, 'url': requested_url, 'requested_url': requested_url,
        'document_url': response.url, 'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        ('header_signature_sha256' if header_credit else 'printed_signature_sha256'):
            hashlib.sha256(signature.encode()).hexdigest(),
        'scope': ('known_public_native_publisher_named_header_editor_private_evidence_only' if header_credit
            else 'known_public_native_publisher_printed_credit_editor_private_evidence_only')}
    return (permit, document) if include_document else permit


def _stockfeel_original_document(response, source, requested_url):
    """Shared public-envelope gate for one-DOM diagnosis and private retention."""
    from downloader.news_fulltext_stockfeel import (
        stockfeel_boundary_source, stockfeel_url_key,
    )
    if (not stockfeel_boundary_source(source) or response.status != 200 or response.redirects
            or response.url != requested_url or stockfeel_url_key(requested_url) is None
            or not isinstance(response.body, bytes) or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES
            or not re.search(r'(?:text/html|application/xhtml[+]xml)',
                response.headers.get('content-type', ''), re.I)):
        raise NewsError('private_stockfeel_original_scope_not_verified')
    return public_article_document(response.body, source, page_url=response.url)


def _stockfeel_original_permit(soup, response, source, requested_url):
    """Private-original gate after the shared public envelope/document gate.

    The resident worker may keep bounded public metadata when this refuses.
    The writer still calls the complete public wrapper on actual raw bytes
    independently before any original is retained.
    """
    from downloader.news_fulltext_stockfeel import (
        STOCKFEEL_BODY_SELECTOR, STOCKFEEL_LAYOUT_VERSIONS, STOCKFEEL_ORIGIN,
        _COPYRIGHT, _clock, stockfeel_url_key,
    )
    from downloader.news_fulltext_public_originals import _visible
    key = stockfeel_url_key(requested_url)
    if any(stockfeel_url_key(value) != key for value in (
            _one(soup, 'head > link[rel=canonical]').get('href'),
            _one(soup, 'head > meta[property="og:url"]').get('content'))):
        raise NewsError('private_stockfeel_original_scope_not_verified')
    scope = _one(soup, '.single-content > .row > .col-md-12 > .sin-content-wrapper')
    header = _one(scope, ':scope > .sin-content-header')
    heading = _one(header, ':scope > h1')
    native = _one(scope, STOCKFEEL_BODY_SELECTOR)
    if not heading.get_text(' ', strip=True) or native.select('iframe,form,[data-paywall],.paywall,vip-content'):
        raise NewsError('private_stockfeel_original_scope_not_verified')
    for node in (scope, header, heading, native):
        _visible(node)
    credits = header.select(':scope > .flex > .flex-wrap > div[data-aid]')
    source_rows = []
    for node in credits:
        spans = node.find_all('span', recursive=False)
        if len(spans) == 2 and spans[0].get_text(' ', strip=True) == '文章來源':
            source_rows.append((node, spans[1]))
    if len(source_rows) != 1:
        raise NewsError('private_stockfeel_original_scope_not_verified')
    node, credit = source_rows[0]
    declared = {'427': ('股感時事特派員', STOCKFEEL_ORIGIN + '/author/news')}
    if STOCKFEEL_LAYOUT_VERSIONS[source.embedded_article_layout] >= 2:
        declared['67'] = ('股感知識庫', STOCKFEEL_ORIGIN + '/author/stockfeel')
    link = _one(credit, ':scope > a')
    if declared.get(node.get('data-aid')) != (link.get_text(' ', strip=True), link.get('href')):
        raise NewsError('private_stockfeel_original_scope_not_verified')
    if (node.has_attr('hidden') or node.has_attr('aria-hidden')
            or re.sub(r'\s+', '', str(node.get('style', ''))).lower() not in {'', 'visibility:hidden;'}):
        raise NewsError('private_stockfeel_original_scope_not_verified')
    for paragraph in native.select('p,blockquote'):
        if re.match(r'^[（(]?(?:轉載自|轉自|本文轉載|本文由|新聞來源[：:])', paragraph.get_text(' ', strip=True)):
            raise NewsError('private_stockfeel_original_scope_not_verified')
    published = _clock(_one(soup, 'head > meta[property="article:published_time"]').get('content'))
    modified = _clock(_one(soup, 'head > meta[property="article:modified_time"]').get('content'))
    notice = _one(soup, 'footer .copyright > span')
    _visible(notice)
    if modified < published or notice.get_text(' ', strip=True) != _COPYRIGHT:
        raise NewsError('private_stockfeel_original_scope_not_verified')
    permit = {'contract_version': 1, 'question': STOCKFEEL_ORIGINAL_QUESTION,
        'provider': 'stockfeel', 'url': requested_url, 'requested_url': requested_url,
        'document_url': response.url, 'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'reviewed_known_public_native_publisher_source_private_identity_credit_evidence_only'}
    return permit


def private_stockfeel_original_permit(response, source, requested_url, *, include_document=False):
    """Private evidence, not a relaxed body parser or retention authorization.

    Prove literal native identity, source, clocks and copyright. Media, unknown
    sources, source refusals and paid/member content remain excluded. Metadata
    alone cannot authorize raw retention; the writer rechecks this full gate.
    """
    soup = _stockfeel_original_document(response, source, requested_url)
    permit = _stockfeel_original_permit(soup, response, source, requested_url)
    return (permit, soup) if include_document else permit


def globalvoices_original_source(source):
    """Registered English native scope only, not WP/profile/media access."""
    return bool(source.provider == 'globalvoices' and source.allowed and source.request_enabled
        and source.authorization.get('kind') == 'open_license'
        and tuple(source.domains) == ('globalvoices.org',)
        and source.title_selector == '.post-header .post-title'
        and source.required_license_url_prefix == 'https://creativecommons.org/licenses/by/3.0/')


def globalvoices_original_url(url, source):
    """One literal registered dated article; never guess routes or aliases."""
    if not globalvoices_original_source(source) or not isinstance(url, str) or not 0 < len(url) <= 8192:
        return False
    try:
        parts = urlsplit(url)
        return (parts.scheme == 'https' and parts.netloc == 'globalvoices.org'
            and not parts.query and not parts.fragment
            and re.fullmatch(r'/[0-9]{4}/[0-9]{2}/[0-9]{2}/[a-z0-9-]+/', parts.path) is not None
            and re.match(source.article_path_pattern, parts.path) is not None)
    except (TypeError, ValueError, re.error):
        return False


def private_globalvoices_original_permit(response, source, requested_url, *, include_document=False):
    """Reuse canonical native CC/author/clock gates, without body admission.

    Photo credits do not become narrative or media rights. No external image,
    linked report, denied author profile, WP API, or active resource is fetched.
    """
    from types import SimpleNamespace
    from downloader.news_fulltext_diagnostics import _licensed_globalvoices_scope

    if (not globalvoices_original_url(requested_url, source) or response.url != requested_url
            or response.status != 200 or response.redirects
            or not isinstance(response.body, bytes) or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES):
        raise NewsError('private_globalvoices_original_scope_not_verified')
    try:
        proved = _licensed_globalvoices_scope(SimpleNamespace(response=response, requested_url=requested_url), source,
            return_document=include_document)
    except (NewsError, ValueError, TypeError, RecursionError):
        proved = False
    if not proved:
        raise NewsError('private_globalvoices_original_scope_not_verified')
    permit = {'contract_version': 1, 'question': GLOBALVOICES_ORIGINAL_QUESTION,
        'provider': 'globalvoices', 'url': requested_url, 'requested_url': requested_url,
        'document_url': response.url, 'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'matching_registered_native_article_CC_authors_clock_private_failure_original_only'}
    return (permit, proved) if include_document else permit


def original_route_version(layout):
    """Only registered native ABIs may use the reviewed literal v11 routes."""
    return 11 if POCKET_LAYOUT_VERSIONS.get(layout, 0) >= 11 else 1


def private_original_permit(response, source):
    """Independently verify own/public native scope, not the failing body grammar.

    This does not grant copyright, publish/training use or infer historical
    availability. Unknown author, alternative body or access-gate evidence
    prevents retention; it is not relaxed just because the parser failed.
    """
    url, payload = response.url, response.body
    route_version = original_route_version(source.embedded_article_layout)
    if (source.provider != 'pocket' or not source.allowed or not source.request_enabled
            or source.authorization.get('kind') != 'personal_research'
            or not source.known_url_only or pocket_article_id(url, adapter_contract_version=route_version) is None
            or response.status != 200 or response.redirects
            or not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ORIGINAL_BYTES):
        raise NewsError('private_original_scope_not_verified')
    if pocket_article_id(url) is None and not pocket_article_transport_verified(
            response, url, adapter_contract_version=route_version):
        raise NewsError('private_original_scope_not_verified')
    soup = public_article_document(payload, source, page_url=url)
    _verify_native_document(soup, url)
    return {'contract_version':1,'question':QUESTION,'provider':'pocket','url':url,
        'response_sha256':hashlib.sha256(payload).hexdigest(),
        'data_fingerprint':source.data_fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
        # Guest-visible native text plus the reviewed personal-research scope
        # is not an invented schema free declaration or a copyright licence.
        'scope':'reviewed_current_public_native_own_organization_private_original_only'}


def _verify_native_document(soup, url):
    """Verify the received document's literal declaration, not a URL lookup."""
    headings = [n for n in _one(soup, 'main').select('h1') if n.find_parent('article') is None]
    if len(headings) != 1:
        raise NewsError('private_original_scope_not_verified')
    heading = headings[0]
    title = heading.get_text(' ', strip=True)
    if (not title or _one(soup, 'head > link[rel=canonical]').get('href') != url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != url
            or _one(soup, 'head > meta[property="og:title"]').get('content') != title
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'):
        raise NewsError('private_original_scope_not_verified')
    header = heading.find_next_sibling()
    if header is None or header.name != 'div':
        raise NewsError('private_original_scope_not_verified')
    credit = _one(header, ':scope > div > span.font-bold.text-slate-600.mr-2')
    native = _one(heading.parent, ':scope > article.cms-body > .cms-article-scope')
    for node in (heading, header, credit, native):
        _native_visible(node)
    if credit.get_text(' ', strip=True) != POCKET_AUTHOR:
        raise NewsError('private_original_scope_not_verified')
    scripts = soup.select('script[type="application/ld+json"]')
    if not 1 <= len(scripts) <= 16:
        raise NewsError('private_original_scope_not_verified')
    declarations = []
    try:
        for script in scripts:
            text = script.get_text()
            if len(text) > 128 << 10:
                raise ValueError('unbounded declaration')
            declarations.extend(a for a in _objects(json.loads(text, object_pairs_hook=_unique_object))
                                if a.get('@type') == 'Article')
    except (TypeError, ValueError, RecursionError):
        raise NewsError('private_original_scope_not_verified') from None
    if len(declarations) != 1:
        raise NewsError('private_original_scope_not_verified')
    article = declarations[0]
    author = article.get('author')
    if (article.get('headline') != title or article.get('url') != url
            or article.get('mainEntityOfPage') != {'@type':'WebPage', '@id':url}
            or ('isAccessibleForFree' in article and article['isAccessibleForFree'] is not True
                and article['isAccessibleForFree'] != 'true') or article.get('articleBody')
            or not isinstance(author, dict) or author.get('@type') != 'Organization'
            or author.get('name') != POCKET_AUTHOR or author.get('url') != POCKET_ORIGIN+'/school/'
            or not isinstance(article.get('publisher'),dict)
            or article['publisher'].get('@type') != 'Organization' or article['publisher'].get('name') != '口袋學堂'):
        raise NewsError('private_original_scope_not_verified')
    first = next((n for n in native.find_all(['p','h2','h3'])
                  if n.find_parent(class_='article-meta') is None), None)
    if first and re.match(r'^(?:轉載|轉自|本文來源|新聞來源|來源[：:])', first.get_text(' ',strip=True)):
        raise NewsError('private_original_scope_not_verified')


def legacy_route(url, *, adapter_contract_version=1):
    """Only a literal known legacy route; canonical questions keep their key."""
    identity = pocket_article_id(url, requested=True, adapter_contract_version=adapter_contract_version)
    return identity is not None and identity.isdecimal() and pocket_article_id(url) is None


def legacy_transport_verified(response, requested_url, *, adapter_contract_version=1):
    """An actual same-ID chain, never an inferred redirect or new GET."""
    if adapter_contract_version == 11 and not legacy_route(requested_url):
        # Additional spellings require the complete received v10 numeric
        # redirect witness; the older source-document-only route stays exact.
        return (legacy_route(requested_url, adapter_contract_version=11)
            and pocket_numeric_transport_verified(response, requested_url))
    identity = pocket_article_id(requested_url, requested=True)
    if not legacy_route(requested_url) or pocket_article_id(response.url, requested=True) != identity:
        return False
    from downloader.news_fulltext_http import RedirectHop
    hops = response.redirects
    if not isinstance(hops, tuple) or any(not isinstance(hop, RedirectHop) for hop in hops):
        return False
    if response.url == requested_url:
        return not hops
    if not 1 <= len(hops) <= 8:
        return False
    previous = requested_url
    for hop in hops:
        if (hop.from_url != previous or hop.status not in {301,302,303,307,308}
                or pocket_article_id(hop.to_url, requested=True) != identity
                or not isinstance(hop.location, str)
                or not isinstance(hop.response_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', hop.response_sha256)
                or type(hop.response_bytes) is not int or not 0 <= hop.response_bytes <= 8 << 20):
            return False
        try:
            if (urljoin(hop.from_url, hop.location) != hop.to_url
                    or datetime.fromisoformat(hop.observed_at_utc).tzinfo is None):
                return False
        except (TypeError, ValueError):
            return False
        previous = hop.to_url
    return previous == response.url


def _legacy_permit(source, response, requested_url, document_url):
    route_version = original_route_version(source.embedded_article_layout)
    if (not legacy_transport_verified(response, requested_url, adapter_contract_version=route_version)
            or pocket_article_id(document_url) != pocket_article_id(requested_url, requested=True,
                adapter_contract_version=route_version)):
        raise NewsError('private_original_scope_not_verified')
    redirects = json.dumps([asdict(hop) for hop in response.redirects], ensure_ascii=False, sort_keys=True)
    return {'contract_version':2,'question':LEGACY_QUESTION,'provider':'pocket',
        'request_url':requested_url,'url':response.url,'document_url':document_url,
        'transport_redirects_sha256':hashlib.sha256(redirects.encode()).hexdigest(),
        'response_sha256':hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint':source.data_fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
        'scope':'reviewed_current_public_native_own_organization_private_legacy_original_only'}


def private_legacy_original_permit(response, source, requested_url):
    """Retain an original ONLY after native same-ID public scope is proved.

    The actual legacy response URL/redirects and the document's separately
    declared canonical URL stay distinct. This never accepts a route alias,
    supplies a guessed canonical URL, or relaxes the production body parser.
    """
    if (source.provider != 'pocket' or not source.allowed or not source.request_enabled
            or source.authorization.get('kind') != 'personal_research' or not source.known_url_only
            or response.status != 200 or not legacy_transport_verified(response, requested_url,
                adapter_contract_version=original_route_version(source.embedded_article_layout))
            or response.headers.get('content-type','').split(';',1)[0].strip().lower() not in {'text/html','application/xhtml+xml'}
            or not isinstance(response.body, bytes) or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES):
        raise NewsError('private_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    document_url = _one(soup, 'head > link[rel=canonical]').get('href')
    permit = _legacy_permit(source, response, requested_url, document_url)
    _verify_native_document(soup, document_url)
    return permit


def _bastille_short_permit(source, response, requested_url, document_url):
    from downloader.news_fulltext_bastillepost import bastillepost_article_id, bastillepost_transport_verified
    if (source.provider != 'bastillepost' or not source.allowed or not source.request_enabled
            or source.authorization.get('kind') != 'personal_research' or not source.known_url_only
            or BASTILLE_LAYOUT_VERSIONS.get(source.embedded_article_layout, 0) < 2
            or not bastillepost_transport_verified(response, requested_url, adapter_contract_version=2)
            or bastillepost_article_id(document_url, adapter_contract_version=2)
                != bastillepost_article_id(requested_url, requested=True, adapter_contract_version=2)):
        raise NewsError('private_original_scope_not_verified')
    redirects = json.dumps([asdict(hop) for hop in response.redirects], ensure_ascii=False, sort_keys=True)
    return {'contract_version': 3, 'question': BASTILLE_SHORT_QUESTION, 'provider': source.provider,
        'request_url': requested_url, 'url': response.url, 'document_url': document_url,
        'transport_redirects_sha256': hashlib.sha256(redirects.encode()).hexdigest(),
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'reviewed_current_public_native_single_short_prose_private_original_only'}


def private_bastille_short_original_permit(response, source, requested_url):
    """Retain one exact native short witness, never relax article acceptance.

    Reuse the current adapter's identity, credit, clock, access and native-tree
    verification. The generic minimum-length rule is deliberately NOT invoked:
    that unresolved rule is exactly why this private original is needed.
    """
    from downloader.news_fulltext_bastillepost import bastillepost_visible_article
    if (response.status != 200 or not isinstance(response.body, bytes)
            or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES
            or response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
                not in {'text/html', 'application/xhtml+xml'}):
        raise NewsError('private_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    document_url = _one(soup, 'head link[rel=canonical]').get('href')
    permit = _bastille_short_permit(source, response, requested_url, document_url)
    _, native = bastillepost_visible_article(soup, page_url=response.url,
        adapter_contract_version=int(source.embedded_article_layout.rsplit('v', 1)[1]))
    paragraphs = native.select('.bastillepost-native-body > p.font-size-scaler-control:not(.caption)')
    if (len(paragraphs) != 1 or not 0 < len(paragraphs[0].get_text('\n', strip=True)) < 100
            or native.get_text('\n', strip=True) != paragraphs[0].get_text('\n', strip=True)):
        raise NewsError('private_original_scope_not_verified')
    return permit


def _bastille_continuation_permit(source, response, requested_url, document_url):
    from downloader.news_fulltext_bastillepost import bastillepost_article_id, bastillepost_transport_verified
    version = BASTILLE_LAYOUT_VERSIONS.get(source.embedded_article_layout, 0)
    if (source.provider != 'bastillepost' or not source.allowed or not source.request_enabled
            or source.authorization.get('kind') != 'personal_research' or not source.known_url_only
            or version < 6 or not bastillepost_transport_verified(response, requested_url, adapter_contract_version=version)
            or bastillepost_article_id(document_url, adapter_contract_version=version)
                != bastillepost_article_id(requested_url, requested=True, adapter_contract_version=version)):
        raise NewsError('private_original_scope_not_verified')
    redirects = json.dumps([asdict(hop) for hop in response.redirects], ensure_ascii=False, sort_keys=True)
    return {'contract_version': 4, 'question': BASTILLE_CONTINUATION_QUESTION, 'provider': source.provider,
        'request_url': requested_url, 'url': response.url, 'document_url': document_url,
        'transport_redirects_sha256': hashlib.sha256(redirects.encode()).hexdigest(),
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'reviewed_current_public_native_two_container_private_original_only'}


def private_bastille_continuation_original_permit(response, source, requested_url):
    """Retain one exact public witness, without guessing a body relationship."""
    from downloader.news_fulltext_bastillepost import bastillepost_private_continuation_scope
    if (response.status != 200 or not isinstance(response.body, bytes)
            or not 0 < len(response.body) <= MAX_ORIGINAL_BYTES
            or response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
                not in {'text/html', 'application/xhtml+xml'}):
        raise NewsError('private_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    document_url = _one(soup, 'head link[rel=canonical]').get('href')
    permit = _bastille_continuation_permit(source, response, requested_url, document_url)
    bastillepost_private_continuation_scope(soup, page_url=response.url,
        adapter_contract_version=BASTILLE_LAYOUT_VERSIONS[source.embedded_article_layout])
    return permit


def retain_original(queue, job, observation):
    """Main-writer durable sidecar; no accepted_batches or article-head writes."""
    from downloader.artifact_io import atomic_write_parquet, table_from_records, sha256_file
    from downloader.news_fulltext_repairs import _bytes, _directory, _write_once
    from downloader.news_fulltext_queue import utc_now
    import pyarrow.parquet as pq

    source, response, permit = job['source'], observation.response, observation.permit
    if not isinstance(permit, dict):
        raise ValueError('private_original_permit_mismatch')
    version = job.get('evidence_contract_version')
    if version == 5:
        expected = _legacy_permit(source, response, job['url'], permit.get('document_url'))
        question = LEGACY_QUESTION
    elif version == 9:
        expected = _bastille_short_permit(source, response, job['url'], permit.get('document_url'))
        question = BASTILLE_SHORT_QUESTION
    elif version == 11:
        expected = _bastille_continuation_permit(source, response, job['url'], permit.get('document_url'))
        question = BASTILLE_CONTINUATION_QUESTION
    elif version == 12:
        from downloader.news_fulltext_pdf import BOARD_PDF_ORIGINAL_QUESTION, private_board_pdf_permit
        expected = private_board_pdf_permit(response, source, job['url'])
        question = BOARD_PDF_ORIGINAL_QUESTION
    elif version == 13:
        expected = private_globalvoices_original_permit(response, source, job['url'])
        question = GLOBALVOICES_ORIGINAL_QUESTION
    elif version == 15:
        expected = private_stockfeel_original_permit(response, source, job['url'])
        question = STOCKFEEL_ORIGINAL_QUESTION
    elif version == 16:
        from downloader.news_fulltext_taiwannews import TAIWANNEWS_ORIGINAL_QUESTION, private_taiwannews_original_permit
        expected = private_taiwannews_original_permit(response, source, job['url'])
        question = TAIWANNEWS_ORIGINAL_QUESTION
    elif version == 17:
        from downloader.news_fulltext_taiwannews import TAIWANNEWS_INLINE_ORIGINAL_QUESTION, private_taiwannews_inline_original_permit
        expected = private_taiwannews_inline_original_permit(response, source, job['url'])
        question = TAIWANNEWS_INLINE_ORIGINAL_QUESTION
    elif version == 18:
        from downloader.news_fulltext_taiwannews import (
            TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION, private_taiwannews_single_root_original_permit)
        expected = private_taiwannews_single_root_original_permit(response, source, job['url'])
        question = TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION
    elif version == 25:
        expected = private_inews_original_permit(response, source, job['url'])
        question = INEWS_ORIGINAL_QUESTION
    elif version == 26:
        expected = private_inews_header_original_permit(response, source, job['url'])
        question = INEWS_HEADER_ORIGINAL_QUESTION
    elif version == 28:
        expected = private_ettoday_credit_original_permit(response, source, job['url'])
        question = ETTODAY_CREDIT_ORIGINAL_QUESTION
    elif version == 29:
        expected = private_ettoday_inline_credit_original_permit(response, source, job['url'])
        question = ETTODAY_INLINE_ORIGINAL_QUESTION
    elif version == 30:
        expected = private_ettoday_visible_credit_original_permit(response, source, job['url'])
        question = ETTODAY_VISIBLE_ORIGINAL_QUESTION
    elif version == 31:
        expected = private_ettoday_image_credit_original_permit(response, source, job['url'])
        question = ETTODAY_IMAGE_ORIGINAL_QUESTION
    elif version == 32:
        expected = private_ettoday_nonempty_credit_original_permit(response, source, job['url'])
        question = ETTODAY_NONEMPTY_ORIGINAL_QUESTION
    elif version == 33:
        expected = private_ettoday_canonical_credit_original_permit(response, source, job['url'])
        question = ETTODAY_CANONICAL_ORIGINAL_QUESTION
    else:
        expected = {'contract_version':1,'question':QUESTION,'provider':'pocket','url':response.url,
            'response_sha256':hashlib.sha256(response.body).hexdigest(),
            'data_fingerprint':source.data_fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
            'scope':'reviewed_current_public_native_own_organization_private_original_only'}
        question = QUESTION
    bound = source.pdf_max_response_bytes if version == 12 else MAX_ORIGINAL_BYTES
    expected_provider = ('ettoday' if version in {28, 29, 30, 31, 32, 33} else 'setn_inews_scope' if version in {25, 26} else 'taiwannews' if version in {16, 17, 18} else 'stockfeel' if version == 15 else 'globalvoices' if version == 13 else 'fed_board' if version == 12
        else 'bastillepost' if version in {9, 11} else 'pocket')
    if (version not in ORIGINAL_EVIDENCE_VERSIONS
            or source.provider != expected_provider
            or not re.fullmatch(r'[0-9a-f]{64}', job['request_id'])
            or response.status != 200 or version == 4 and (response.url != job['url'] or response.redirects)
            or not 0 < len(response.body) <= bound
            or permit != expected):
        raise ValueError('private_original_permit_mismatch')
    directory = _directory(queue.root,'candidate_diagnostics','originals')
    path = directory/(job['request_id']+'.parquet')
    receipt_path = directory/(job['request_id']+'.receipt.json')
    if path.exists() or path.is_symlink() or receipt_path.exists() or receipt_path.is_symlink():
        raise ValueError('private_original_evidence_already_exists')
    flags = {'diagnostic_only':True,'accepted_as_article':False,'publish':False,
             'historical_point_in_time':False,'per_article_license_verified':False,
             'training_injection_enabled':False,'remote_training_export':False}
    record = {'request_id':job['request_id'],'provider':source.provider,'request_url':job['url'],
        'final_url':response.url,'observed_at_utc':utc_now(),'raw_response_body':response.body,
        'response_sha256':permit['response_sha256'],'data_fingerprint':source.data_fingerprint,
        'authorization_fingerprint':source.authorization_fingerprint,
        'headers_json':json.dumps({k:v for k,v in response.headers.items()
            if k.lower() in {'date','content-type','last-modified'}},ensure_ascii=False),**flags}
    if version in {5, 9, 11, 12, 13, 15, 16, 17, 18, 25, 28, 29, 30, 31, 32, 33}:
        record.update(source_document_url=permit['document_url'],
            transport_redirects_json=json.dumps([asdict(hop) for hop in response.redirects],ensure_ascii=False,sort_keys=True))
    atomic_write_parquet(path,table_from_records([record]),compression='zstd',compression_level=9,durable=True)
    path.chmod(0o600)
    restored = pq.ParquetFile(path).read().to_pylist()
    if restored != [record]:
        raise ValueError('private_original_readback_mismatch')
    reference = {'path':str(path.relative_to(queue.root)),'sha256':sha256_file(path),
        'receipt_path':str(receipt_path.relative_to(queue.root)),
        'response_sha256':permit['response_sha256'],'response_bytes':len(response.body)}
    _write_once(receipt_path,_bytes({'contract_version':1,'request_id':job['request_id'],
        'question':question,'permit':permit,'original':reference,**flags}))
    return {**observation.facts,'raw_or_names_or_body_retained':True,
        'raw_retention_state':'retained_verified','private_original':reference,
        'article_heads_mutated':False,'publish':False,'remote_training_export':False}


def retention_flags(request, result, state):
    """Uncertain outcomes must not falsely promise that no raw witness exists."""
    if request.get('evidence_contract_version',1) not in ORIGINAL_EVIDENCE_VERSIONS:
        return {'raw_or_names_or_body_retained':False}
    if result is not None:
        return {'raw_or_names_or_body_retained':result.get('raw_or_names_or_body_retained',False),
                'raw_retention_state':result.get('raw_retention_state','not_retained')}
    uncertain = state in {'reserved','uncertain_outcome'}
    return {'raw_or_names_or_body_retained':None if uncertain else False,
            'raw_retention_state':'unknown' if uncertain else 'not_retained'}
