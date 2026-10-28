"""Mega News native TagDiv article, using the shared HTTP and text walkers.

The WordPress template ID is not the article ID. Bind the literal canonical
and main-entity URL instead; retain the source's visible author spelling and
separate publication/modification clocks. No requests or endpoint inference.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import re
from urllib.parse import unquote, urlsplit

from bs4 import Comment

from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_public_originals import _one, _visible


CONTENTPLATFORM_PATH = r'^/articles/[1-9][0-9]*/(?:[A-Za-z0-9_-]|%[0-9A-Fa-f]{2})+/$'
CONTENTPLATFORM_BODY = ':scope > .contentplatform-verified-body'
CONTENTPLATFORM_TITLE = 'h1.tdb-title-text'
CONTENTPLATFORM_ORIGINAL_LAYOUT = 'contentplatform_native_article_v2'
CONTENTPLATFORM_ORIGINAL_LAYOUTS = frozenset({CONTENTPLATFORM_ORIGINAL_LAYOUT, 'contentplatform_native_article_v3'})
CONTENTPLATFORM_KNOWN_NATIVE_LAYOUT = 'contentplatform_native_article_v4'
CONTENTPLATFORM_WRAPPED_NATIVE_LAYOUT = 'contentplatform_native_article_v5'
CONTENTPLATFORM_WRAPPED_MEDIA_LAYOUT = 'contentplatform_native_article_v6'
CONTENTPLATFORM_WRITTEN_HTML_LAYOUT = 'contentplatform_native_article_v7'
CONTENTPLATFORM_STATIC_DOCUMENT_LAYOUT = 'contentplatform_native_article_v8'
CONTENTPLATFORM_V5_VERIFICATION = 'literal_native_written_div_and_block_image_geometry_v5_retained_20261026'
CONTENTPLATFORM_V6_VERIFICATION = 'native_known_written_wrappers_and_wordpress_media_placement_v6_20261026'
CONTENTPLATFORM_V7_VERIFICATION = 'native_full_written_static_HTML_media_not_CSS_identity_v7_20261026'
CONTENTPLATFORM_V8_VERIFICATION = 'native_static_written_HTML_script_excluded_empty_inert_leaf_v8_20261026'
CONTENTPLATFORM_V4_AUTH_SCOPE = (
    'User-requested private personal noncommercial downloading of already locally known public Mega News '
    'written articles, under the official copyright page personal noncommercial downloading/copying clause '
    'reviewed2026-10-26. Literal canonical/mainEntity, publisher, visible author and Person/profile declaration, '
    'separate explicit publication/modification clocks and complete visible written body remain verified. '
    'The publisher account and every in-body reporter/company/source credit are distinct original declarations; '
    'neither a signed-credit template nor a profile proves copyright ownership. Preserve original raw responses, '
    'values, source credits and clocks. No site discovery, guessed endpoints, raw media, paid/member content, '
    'alias merge, public redistribution, automatic model training or remote export; CNA acquisition remains '
    'deferred. This is private reading/download scope, not an open license, ownership finding or historical '
    'first-publication guarantee.')
CONTENTPLATFORM_V4_SCOPE_CUTOFF = '2026-10-26T09:40:00+00:00'
CONTENTPLATFORM_V4_VERIFICATION = 'literal_native_identity_account_and_full_written_credits_v4_20261026'
CONTENTPLATFORM_V4_NOTE = (
    'Known local public articles only; official personal noncommercial download clause. '
    'Publisher byline, signed reporter and company/source credits are separate raw declarations. '
    'No discovery, article aliases, redistribution, remote training export or source-limit reset. '
    'Existing single-owner pacing remains a seed, not a measured maximum.')
# New capture/auth, original HTTP capture/auth, and the actual v3 capture.
# Only this exact reviewed change inherits the old HTTP/refusal identity.
CONTENTPLATFORM_V4_HTTP_BINDING = (
    'c2a2e2c77347959774d9695ebee68017ec4b11cf8cd2ed24f2027c47098f7f0a',
    '71d6de6fb04ee4a2a34188bd244394d5dc07a580ebd0ec45ecc672498506d454',
    '2041f024b49da085fec56a04c32e4ce7f44d0278bc0ade15fdaeb4f93f4baeaa',
    '3cb1682113f803750f4cba8fa0cfe5b6ba7b2a79ce51683f32452977536fe7d4',
    'd6701f71a7eadcaf6622112c3e2f15fa17bc70e9e3ff7f1fcc5be701c0e2431a',
)
# Current geometry, unchanged authorization, and original failed capture.
CONTENTPLATFORM_V5_ORIGINAL_BINDING = (
    '88d827bfbf935e5e2e3acd3bda6af5799396b426e8b53719bc6d6513f07fbb41',
    CONTENTPLATFORM_V4_HTTP_BINDING[1], CONTENTPLATFORM_V4_HTTP_BINDING[0],
)
CONTENTPLATFORM_V6_CAPTURE = '9e6f3fb6092de6b531abeb923adaf04eb2f71833dd4a9aa5753163b9d5e95e54'
CONTENTPLATFORM_V7_CAPTURE = '98e2b9c1f31fbc91f3544de059439f1dbf00ce756a034d0a034d2e4244cc7ae9'
CONTENTPLATFORM_ORIGINAL_QUESTION = 'contentplatform_private_signed_native_geometry_original_v1'
CONTENTPLATFORM_ROOT_QUESTION = 'contentplatform_private_leading_nested_credit_root_original_v1'
CONTENTPLATFORM_IMAGE_QUESTION = 'contentplatform_private_first_written_credit_after_leading_image_original_v1'
CONTENTPLATFORM_PREFIX_QUESTION = 'contentplatform_private_first_written_signed_prefix_original_v1'
CONTENTPLATFORM_FAILURE_SHAPE_QUESTION = 'contentplatform_current_native_failure_shape_metadata_v1'
CONTENTPLATFORM_EVIDENCE_QUESTIONS = {
    43: CONTENTPLATFORM_ORIGINAL_QUESTION,
    44: CONTENTPLATFORM_ROOT_QUESTION,
    45: CONTENTPLATFORM_IMAGE_QUESTION,
    53: CONTENTPLATFORM_PREFIX_QUESTION,
}
CONTENTPLATFORM_EVIDENCE_VERSIONS = frozenset(CONTENTPLATFORM_EVIDENCE_QUESTIONS)
# Captured by the canonical resident owner before the v3 media-only repair.
# Bind retained reuse to its actual source/auth identity, never to a new GET.
CONTENTPLATFORM_V2_ORIGINAL_BINDING = (
    'c69acca09425697d433e653a61c3df99c04a979ca29d457a837d789b06370b05',
    '3cb1682113f803750f4cba8fa0cfe5b6ba7b2a79ce51683f32452977536fe7d4',
)
# One already retained, independently read-back original. Preserve its former
# diagnostic provider and authorization, rather than fetching or relabelling it.
# Tuple: literal URL, raw response SHA, original provider, canonical auth SHA.
CONTENTPLATFORM_RETAINED_REGISTRATIONS = ((
    'https://www.contentplatform.info/articles/533649/jaya533649/',
    '44985ffbc9511c078cbec3ae9d515dbc70f76554465b4e4b78abbede286e0c8b',
    'policy_contentplatform_review',
    '2bc9d2a70eb12074391b0372ed3b5b40dd35d6d5bede62e00f972a0180a0cce5',
),)
_TEXT_TAGS = frozenset({'p', 'br', 'a', 'b', 'strong', 'i', 'em', 'u', 's', 'span',
    'h2', 'h3', 'h4', 'ul', 'ol', 'li', 'blockquote', 'table', 'thead', 'tbody',
    'tfoot', 'tr', 'td', 'th', 'sup', 'sub'})
# These standard static written elements retain every descendant text value.
# Void separators contribute no prose. Active/embedded content and unknown
# elements remain rejected; CSS classes cannot establish article identity.
_WRITTEN_HTML_TAGS = _TEXT_TAGS | frozenset({'div', 'section', 'address',
    'h1', 'h5', 'h6', 'hr', 'wbr', 'pre', 'code', 'kbd', 'samp', 'var',
    'small', 'mark', 'del', 'ins', 'abbr', 'cite', 'q', 'dfn', 'time',
    'dl', 'dt', 'dd', 'caption', 'colgroup', 'col'})
# A textless leaf is not missing prose. Only inert presentational attributes
# qualify; embedded resources, controls, custom data and events remain unknown.
_EMPTY_LEAF_ATTRIBUTES = frozenset({'id', 'class', 'style', 'title', 'lang', 'dir',
    'aria-hidden', 'color', 'size', 'face'})
_NON_STATIC_ELEMENTS = frozenset({'script', 'style', 'iframe', 'frame', 'frameset',
    'object', 'embed', 'applet', 'canvas', 'svg', 'math', 'picture', 'source',
    'video', 'audio', 'track', 'form', 'input', 'textarea', 'select', 'option',
    'button', 'link', 'meta', 'base', 'template', 'noscript'})


def _literal_identity(url):
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, TypeError, ValueError):
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.contentplatform.info'
            or parts.query or parts.fragment or not re.fullmatch(CONTENTPLATFORM_PATH, parts.path)
            or re.search(r'%(?:2f|5c)', parts.path, re.I)):
        return None
    try:
        # UTF-8 percent hex case/escaped typography corroborates the literal
        # same path only. Never change requests, slugs or the canonical URL.
        return unquote(parts.path, errors='strict')
    except UnicodeError:
        return None


def _clock(value):
    if not isinstance(value, str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+08:00', value) is None:
        raise NewsError('invalid_bulk_publication_clock')
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None


def contentplatform_article_id(url, *, requested=False):
    """Exact native path for repair eligibility; never rewrites a request."""
    path = _literal_identity(url)
    return path.split('/')[2] if path is not None else None


def contentplatform_original_source(source):
    """The current registered native scope, without a parser/scope upgrade."""
    return bool(source and source.provider == 'contentplatform' and source.allowed and source.request_enabled
        and source.known_url_only and source.acquisition_mode == 'continuous'
        and source.embedded_article_layout in CONTENTPLATFORM_ORIGINAL_LAYOUTS
        and source.domains == ('www.contentplatform.info',)
        and source.article_path_pattern == CONTENTPLATFORM_PATH
        and source.selectors == (CONTENTPLATFORM_BODY,) and source.title_selector == CONTENTPLATFORM_TITLE
        and source.require_configured_container and source.require_author_attribution
        and not any((source.feeds, source.fulltext_feeds, source.wordpress_apis, source.index_pages,
            source.incremental_index_pages, source.metadata_feeds, source.cnyes_apis, source.exclude_selectors))
        and source.authorization.get('kind') == 'personal_research'
        and source.authorization.get('evidence_url') == 'https://www.contentplatform.info/copyright/')


def contentplatform_known_native_source(source):
    """Personal reading scope; attribution is evidence, not a rights regex."""
    return bool(source and source.embedded_article_layout in {
            CONTENTPLATFORM_KNOWN_NATIVE_LAYOUT, CONTENTPLATFORM_WRAPPED_NATIVE_LAYOUT,
            CONTENTPLATFORM_WRAPPED_MEDIA_LAYOUT, CONTENTPLATFORM_WRITTEN_HTML_LAYOUT,
            CONTENTPLATFORM_STATIC_DOCUMENT_LAYOUT}
        and contentplatform_original_source(replace(source, embedded_article_layout='contentplatform_native_article_v3'))
        and source.authorization.get('scope') == CONTENTPLATFORM_V4_AUTH_SCOPE
        and source.authorization.get('reviewed_on') == '2026-10-26')


def _native_frame(soup, *, page_url):
    expected = _literal_identity(page_url)
    canonical = _one(soup, 'head link[rel="canonical"][href]')
    article = _one(soup, 'article[itemtype="https://schema.org/Article"]')
    entity = _one(article, ':scope > meta[itemprop="mainEntityOfPage"][itemid]')
    if (expected is None or _literal_identity(canonical['href']) != expected
            or _literal_identity(entity['itemid']) != expected):
        raise NewsError('article_identity_not_verified')
    heading = _one(article, CONTENTPLATFORM_TITLE, 'article_title_not_verified')
    headline = _one(article, ':scope > meta[itemprop="headline"][content]', 'article_title_not_verified')
    title = heading.get_text(' ', strip=True)
    if not title or title != headline['content']:
        raise NewsError('article_title_not_verified')
    publisher = _one(article, ':scope > span[itemprop="publisher"] > meta[itemprop="name"][content]')
    if publisher['content'] != '報新聞 Mega News':
        raise NewsError('article_identity_not_verified')

    author = _one(article, '.tdb_single_author .tdb-author-name[href]', 'article_attribution_not_verified')
    declaration = _one(article, ':scope > span[itemprop="author"][itemtype="https://schema.org/Person"]',
                       'article_attribution_not_verified')
    name = author.get_text(' ', strip=True)
    declared_name = _one(declaration, ':scope > meta[itemprop="name"][content]', 'article_attribution_not_verified')
    profile = _one(declaration, ':scope > meta[itemprop="url"][content]', 'article_attribution_not_verified')
    if (not name or name != declared_name['content'] or author['href'] != profile['content']
            or not re.fullmatch(r'https://www[.]contentplatform[.]info/articles/author/[A-Za-z0-9_-]+/', author['href'])):
        raise NewsError('article_attribution_not_verified')
    publication = _one(article, ':scope > meta[itemprop="datePublished"][content]', 'invalid_bulk_publication_clock')['content']
    modified = _one(article, ':scope > meta[itemprop="dateModified"][content]', 'invalid_bulk_publication_clock')['content']
    clock = _one(article, '.tdb_single_date time[datetime]', 'invalid_bulk_publication_clock')
    if (clock['datetime'] != publication or clock.get_text(' ', strip=True) != publication[:10]
            or _clock(modified) < _clock(publication)):
        raise NewsError('invalid_bulk_publication_clock')

    body = _one(article, '.tdb_single_content > .tdb-block-inner')
    for node in (article, heading, author, clock, body):
        _visible(node)
    return {
        'headline': title, 'author': [{'name': name, 'url': author['href']}],
        'datePublished': publication, 'dateModified': modified,
        'published_time_basis': 'matched_native_time_and_microdata; original_explicit_offset_and_precision_preserved',
        'article_identity_basis': 'literal_canonical_and_main_entity_URL; native_template_ID_not_used_as_article_ID',
    }, body


def _signed_credit(credit, name):
    signed = re.fullmatch(r'【([^【】]+)記者\s*/\s*綜合報導】', credit)
    return bool(signed and re.sub(r'\s+', '', signed[1]) == re.sub(r'\s+', '', name))


def _native_header(soup, *, page_url, include_failure_context=False):
    metadata, body = _native_frame(soup, page_url=page_url)
    paragraphs = body.find_all('p', recursive=False)
    if not paragraphs:
        failure = NewsError('article_identity_not_verified')
    elif not _signed_credit(paragraphs[0].get_text(' ', strip=True), metadata['author'][0]['name']):
        failure = NewsError('article_rights_or_scope_excluded')
    else:
        failure = None
    # Keep the account spelling and signed name intact; whitespace only
    # corroborates their relation and never rewrites an accepted author.
    if failure is not None:
        if include_failure_context:
            failure.private_scope_facts = {'native_header_failure_context':
                _header_failure_context(metadata, body)}
        raise failure
    metadata['body_completeness'] = 'verified_current_native_signed_written_prose; media_and_captions_excluded'
    return metadata, body


def contentplatform_root_probe(response, source, *, include_context=False):
    """Inspect a new leading-root question without accepting article geometry.

    Only the first nonempty child at each level can establish a credit; never
    search later paragraphs for a convenient name. Unknown containers still
    yield bounded tag/boolean metadata. No title, byline or prose is exported.
    """
    from downloader.news_fulltext_policy import public_article_document
    if not contentplatform_original_source(source) or response.status != 200:
        raise NewsError('private_contentplatform_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    metadata, body = _native_frame(soup, page_url=response.url)
    counts = {}
    for child in body.find_all(recursive=False):
        counts[child.name] = min(129, counts.get(child.name, 0) + 1)
        if len(counts) > 32:
            raise NewsError('private_contentplatform_root_shape_limit')
    current, path, credit = body, [], None
    for _ in range(4):
        first = next((child for child in current.contents
                      if getattr(child, 'name', None) or str(child).strip()), None)
        if first is None or isinstance(first, Comment) or not getattr(first, 'name', None):
            break
        _visible(first)
        path.append(first.name)
        if first.name == 'p':
            credit = first
            break
        if first.name != 'div':
            break
        current = first
    nested = bool(credit is not None and len(path) > 1 and not counts.get('p'))
    matched = bool(nested and _signed_credit(credit.get_text(' ', strip=True), metadata['author'][0]['name']))
    facts = {'contract_version': 1, 'native_identity_author_clock_verified': True,
        'direct_tag_counts': counts, 'first_written_candidate_path': path,
        'first_candidate_is_nested_paragraph': nested, 'first_credit_matches_declared_author': matched,
        'body_geometry_accepted': False, 'article_accepted': False, 'source_text_exported': False}
    return (facts, body) if include_context else facts


def contentplatform_private_root_permit(response, source):
    facts, body = contentplatform_root_probe(response, source, include_context=True)
    try:
        if (not facts['first_credit_matches_declared_author']
                or body.select('script, iframe, form, input, video, audio')):
            raise NewsError('private_contentplatform_nested_credit_not_verified')
        for node in body.find_all(True):
            _visible(node)
    except NewsError as failure:
        failure.private_scope_facts = {'native_root_shape': facts}
        raise
    return {**facts, 'signed_reporter_and_explicit_clocks_verified': True}


def contentplatform_image_probe(response, source, *, include_context=False):
    """Find the first written paragraph in document order after leading media.

    Walk only bounded transparent divs and images. A written text node, other
    element or first paragraph stops the walk, even if a later credit matches.
    No body is accepted, media fetched, or source text exported by this probe.
    """
    from downloader.news_fulltext_policy import public_article_document
    if not contentplatform_original_source(source) or response.status != 200:
        raise NewsError('private_contentplatform_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    metadata, body = _native_frame(soup, page_url=response.url)
    facts, _ = _first_written_shape(metadata, body)
    return (facts, body) if include_context else facts


def _first_written_shape(metadata, body, *, media_figures=False):
    """Bounded same-DOM evidence; the first paragraph is never skipped."""
    stack, images, credit, stopped, walked, comments = [(iter(body.contents), [])], [], None, None, 0, 0
    figures = []
    while stack and walked < 64:
        children, path = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            continue
        walked += 1
        if isinstance(child, Comment):
            comments += 1
            continue  # Non-rendered; retained raw still contains the comment.
        tag = getattr(child, 'name', None)
        if not tag:
            if str(child).strip():
                stopped = [*path, '#text']
                break
            continue
        _visible(child)
        stopped = [*path, tag]
        if tag == 'div' and len(path) < 4:
            stack.append((iter(child.contents), stopped))
        elif tag == 'img' and len(images) < 4:
            images.append(stopped)
        elif media_figures and tag == 'figure' and len(images) < 4:
            # Only a textless, one-image unit can precede the first written
            # paragraph. Captions, links and unknown descendants stop here.
            descendants = list(child.find_all(True))
            if (len(descendants) != 1 or descendants[0].name != 'img'
                    or child.get_text(strip=True)):
                break
            _visible(descendants[0])
            images.append([*stopped, 'img'])
            figures.append(stopped)
        else:
            if tag == 'p':
                credit = child
            break
    exhausted = bool(stack and walked >= 64 and credit is None)
    matched = bool(images and credit is not None and _signed_credit(
        credit.get_text(' ', strip=True), metadata['author'][0]['name']))
    facts = {'contract_version': 1, 'native_identity_author_clock_verified': True,
        'leading_image_paths': images, 'first_written_candidate_path': stopped,
        'first_candidate_is_paragraph': credit is not None,
        'first_credit_matches_declared_author': matched, 'walked_nodes': walked,
        'leading_comments': comments, 'walk_limit_reached': exhausted,
        'body_geometry_accepted': False, 'article_accepted': False, 'source_text_exported': False}
    if media_figures:
        facts['leading_figure_paths'] = figures
    return facts, credit


def _header_failure_context(metadata, body):
    """Preserve diagnostic facts from the parsed input, without source text.

    A signed prefix followed by prose is evidence only. It cannot establish
    the old full-paragraph permit, change the article parser or reopen a job.
    """
    try:
        facts, credit = _first_written_shape(metadata, body)
    except NewsError:
        return {'contract_version': 1, 'native_identity_author_clock_verified': True,
            'leading_shape_visible': False, 'source_text_exported': False,
            'article_accepted': False, 'body_geometry_accepted': False}
    text = credit.get_text(' ', strip=True) if credit is not None else ''
    end = text.find('】', 0, 256)
    signed_prefix = bool(end >= 0 and _signed_credit(text[:end+1], metadata['author'][0]['name']))
    # Unknown HTML names can contain arbitrary source words. This context
    # exports only standard structural tags and a fixed unknown marker.
    tags = _TEXT_TAGS | {'div', 'img', 'figure', '#text'}
    path = facts['first_written_candidate_path']
    return {'contract_version': 1, 'native_identity_author_clock_verified': True,
        'leading_shape_visible': True, 'leading_image_count': len(facts['leading_image_paths']),
        'first_written_candidate_path': [t if t in tags else '#other' for t in path] if path else None,
        'first_candidate_is_paragraph': credit is not None,
        'whole_paragraph_credit_matches_declared_author': bool(credit is not None and
            _signed_credit(text, metadata['author'][0]['name'])),
        'signed_prefix_matches_declared_author': signed_prefix,
        'signed_prefix_followed_by_prose': bool(signed_prefix and text[end+1:].strip()),
        'walk_limit_reached': facts['walk_limit_reached'], 'source_text_exported': False,
        'article_accepted': False, 'body_geometry_accepted': False}


def contentplatform_private_image_permit(response, source):
    facts, body = contentplatform_image_probe(response, source, include_context=True)
    try:
        if (not facts['first_credit_matches_declared_author'] or facts['walk_limit_reached']
                or body.select('script, iframe, form, input, video, audio')):
            raise NewsError('private_contentplatform_post_image_credit_not_verified')
        for node in body.find_all(True):
            _visible(node)
    except NewsError as failure:
        failure.private_scope_facts = {'native_image_credit_shape': facts}
        raise
    return {**facts, 'signed_reporter_and_explicit_clocks_verified': True}


def contentplatform_private_prefix_permit(response, source):
    """Keep one original when the first written paragraph starts signed.

    A complete native frame and a corroborated literal reporter prefix permit
    private diagnosis only. Never scan later prose for a matching name or
    change the article parser's full-paragraph/body-geometry requirements.
    """
    from downloader.news_fulltext_policy import public_article_document
    if not contentplatform_original_source(source) or response.status != 200:
        raise NewsError('private_contentplatform_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    metadata, body = _native_frame(soup, page_url=response.url)
    facts, credit = _first_written_shape(metadata, body, media_figures=True)
    text = credit.get_text(' ', strip=True) if credit is not None else ''
    end = text.find('】', 0, 256)
    prefix = bool(end >= 0 and _signed_credit(text[:end+1], metadata['author'][0]['name']))
    tags = _TEXT_TAGS | {'div', 'img', 'figure', '#text'}
    path = facts['first_written_candidate_path']
    facts['first_written_candidate_path'] = [t if t in tags else '#other' for t in path] if path else None
    facts.update(signed_prefix_matches_declared_author=prefix,
        signed_prefix_followed_by_prose=bool(prefix and text[end+1:].strip()),
        whole_paragraph_credit_matches_declared_author=bool(credit is not None
            and _signed_credit(text, metadata['author'][0]['name'])))
    try:
        if (not prefix or facts['walk_limit_reached']
                or body.select('script, iframe, form, input, video, audio')):
            raise NewsError('private_contentplatform_signed_prefix_not_verified')
        for node in body.find_all(True):
            _visible(node)
    except NewsError as failure:
        failure.private_scope_facts = {'native_prefix_credit_shape': facts}
        raise
    return {**facts, 'signed_reporter_and_explicit_clocks_verified': True}


def _written_media_figure(figure, body, *, legacy_caption=False, require_wordpress_class=True):
    """Static image/caption unit; prose outside a caption disqualifies it."""
    allowed_parents = {'div'} if require_wordpress_class else _WRITTEN_HTML_TAGS
    parent = figure.parent
    while parent is not body:
        if parent is None or parent.name not in allowed_parents:
            return False
        parent = parent.parent
    classes = {'wp-block-image', 'wp-caption'} if legacy_caption else {'wp-block-image'}
    if ((require_wordpress_class and not classes.intersection(figure.get('class', [])))
            or len(figure.find_all('img')) != 1 or len(figure.find_all('figcaption')) > 1):
        return False
    for child in figure.find_all(True):
        if child.name not in {'img', 'a', 'figcaption', 'br', 'em', 'strong', 'span'}:
            return False
        _visible(child)
    # Distinguish an actual caption from prose beside an image; never discard
    # that prose as a media unit.
    return all(not str(text).strip() or text.find_parent('figcaption') is not None
        for text in figure.find_all(string=True))


def _empty_inert_leaf(node):
    return (node.name not in _NON_STATIC_ELEMENTS
        and set(node.attrs) <= _EMPTY_LEAF_ATTRIBUTES
        and not node.find_all(True) and not node.get_text(strip=True))


def contentplatform_native_article(soup, *, page_url, adapter_contract_version=1):
    if adapter_contract_version not in {1, 2, 3, 4, 5, 6, 7, 8}:
        raise ValueError('unreviewed Contentplatform adapter version')
    if adapter_contract_version >= 4:
        metadata, body = _native_frame(soup, page_url=page_url)
        # Account and in-body credits are distinct declarations. Keep every
        # written credit; do not infer its owner or require one byline format.
        paragraphs = body.find_all('p')
        first = paragraphs[0].get_text(' ', strip=True) if paragraphs else ''
        if ('中央社' in metadata['author'][0]['name']
                or re.match(r'^[（(【]?\s*(?:中央社(?:記者|電|／|/|提供|授權)|CNA\b)', first)):
            raise NewsError('article_rights_or_scope_excluded')
        metadata['body_completeness'] = (
            'verified_current_native_written_prose; publisher_byline_and_body_credits_distinct; media_and_captions_excluded')
        if adapter_contract_version >= 8:
            metadata['body_completeness'] += '; static_HTML_only; non_rendered_scripts_and_empty_inert_leaves_excluded'
    else:
        metadata, body = _native_header(soup, page_url=page_url)
    figures = []
    images = []
    non_written = []
    allowed_tags = _WRITTEN_HTML_TAGS if adapter_contract_version >= 7 else _TEXT_TAGS
    for node in body.find_all(True):
        if node.name == 'figure':
            if adapter_contract_version >= 6:
                _visible(node)
            # Exclude this observed media/caption unit, never unknown prose.
            legacy = (adapter_contract_version < 7 and node.parent is body and 'wp-caption' in node.get('class', [])
                and node.find('img') and not node.select('p, h1, h2, h3, table, blockquote, script, iframe'))
            if not legacy and not (adapter_contract_version >= 5 and _written_media_figure(
                    node, body, legacy_caption=adapter_contract_version >= 6,
                    require_wordpress_class=adapter_contract_version < 7)):
                raise NewsError('article_identity_not_verified')
            figures.append(node)
        elif node.name == 'img' and node.find_parent('figure') is None:
            parent = node.parent
            # The retained native v3 witness uses an empty h3 as an image
            # wrapper. Written headings, mixed prose and unknown media trees
            # must still fail instead of losing words from accepted text.
            media_parents = {'p', 'h3'} if adapter_contract_version >= 3 else {'p'}
            known_image = (adapter_contract_version < 7 and any(
                re.fullmatch(r'wp-image-[1-9][0-9]*', name) for name in node.get('class', [])))
            if adapter_contract_version >= 7:
                # An HTML img is a void media node, independent of a styling
                # class. Its parent and siblings keep all written text.
                valid_image = not node.find_all(True)
            elif adapter_contract_version >= 6:
                # Only the image is removed. Keep its written parent and all
                # siblings, even when WordPress places it within a div or
                # inline prose. The ordinary loop validates every parent tag
                # and its visibility; no unknown subtree becomes transparent.
                valid_image = known_image and not node.find_all(True)
            else:
                valid_image = (adapter_contract_version >= 2 and parent.name in media_parents
                    and parent.parent is body and not parent.get_text(strip=True)
                    and len(parent.find_all('img', recursive=False)) == 1
                    and not any(child.name != 'img' for child in parent.find_all(True)) and known_image)
            if not valid_image:
                raise NewsError('article_identity_not_verified')
            _visible(node)
            images.append(node)
        elif node.find_parent('figure') is None:
            if adapter_contract_version >= 8 and (node.name == 'script'
                    or node.name not in allowed_tags and _empty_inert_leaf(node)):
                # WHATWG: script does not represent user content. Never run it,
                # fetch its src, decode data blocks or infer generated prose.
                # Keep the original raw and all written siblings/parents.
                non_written.append(node)
            elif node.name not in allowed_tags and not (adapter_contract_version >= 5 and node.name == 'div'):
                raise NewsError('article_identity_not_verified')
            _visible(node)
    if any(isinstance(node, Comment) for node in body.descendants):
        raise NewsError('article_identity_not_verified')
    if any(str(node).strip() for node in body.contents if not getattr(node, 'name', None)):
        raise NewsError('article_identity_not_verified')
    for figure in figures:
        figure.decompose()
    for image in images:
        image.decompose()
    for node in non_written:
        node.decompose()
    body.attrs['class'] = ['contentplatform-verified-body']
    return metadata, body.parent


def contentplatform_private_failure_permit(response, source):
    """A checked signed public header permits diagnosis, never body acceptance.

    Unknown written/media geometry remains a failed article. Retain only an
    already received, same-literal public response under the reviewed source;
    this helper performs no requests and exports no title, byline or prose.
    """
    from downloader.news_fulltext_policy import public_article_document
    if (source.provider != 'contentplatform' or not source.allowed or not source.known_url_only
            or source.acquisition_mode != 'continuous'
            or source.embedded_article_layout not in {
                'contentplatform_native_article_v1', *CONTENTPLATFORM_ORIGINAL_LAYOUTS,
                CONTENTPLATFORM_KNOWN_NATIVE_LAYOUT, CONTENTPLATFORM_WRAPPED_NATIVE_LAYOUT,
                CONTENTPLATFORM_WRAPPED_MEDIA_LAYOUT, CONTENTPLATFORM_WRITTEN_HTML_LAYOUT,
                CONTENTPLATFORM_STATIC_DOCUMENT_LAYOUT}
            or source.domains != ('www.contentplatform.info',)
            or source.authorization.get('kind') != 'personal_research'
            or source.authorization.get('evidence_url') != 'https://www.contentplatform.info/copyright/'
            or response.status != 200 or _literal_identity(response.url) is None
            or not re.search(r'(?:text/html|application/xhtml[+]xml)', response.headers.get('content-type', ''), re.I)):
        raise NewsError('private_contentplatform_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    native = contentplatform_known_native_source(source)
    if source.embedded_article_layout in {CONTENTPLATFORM_KNOWN_NATIVE_LAYOUT, CONTENTPLATFORM_WRAPPED_NATIVE_LAYOUT,
            CONTENTPLATFORM_WRAPPED_MEDIA_LAYOUT, CONTENTPLATFORM_WRITTEN_HTML_LAYOUT,
            CONTENTPLATFORM_STATIC_DOCUMENT_LAYOUT} and not native:
        raise NewsError('private_contentplatform_original_scope_not_verified')
    if native:
        _, body = _native_frame(soup, page_url=response.url)
    else:
        _, body = _native_header(soup, page_url=response.url, include_failure_context=True)
    if body.select('script, iframe, form, input, video, audio'):
        raise NewsError('private_contentplatform_original_scope_not_verified')
    for node in body.find_all(True):
        _visible(node)
    return {'contract_version': 1, 'literal_native_identity_verified': True,
        **({'native_publisher_byline_and_explicit_clocks_verified': True} if native else
           {'signed_reporter_and_explicit_clocks_verified': True}),
        'body_geometry_accepted': False, 'article_accepted': False}


def contentplatform_failure_shape(soup, *, page_url):
    """One bounded structural question, without names, text, raw or acceptance.

    Native-frame refusals remain refusals. Geometry is examined only after
    that frame passes. No class/URL words or arbitrary element names escape.
    """
    from collections import Counter
    from downloader.news_fulltext_candidates import extraction_failure_location
    facts = {'contract_version': 1, 'native_frame_verified': False,
        'body_geometry_accepted': False, 'article_accepted': False, 'source_text_exported': False}
    facts['frame_node_counts'] = {name:len(soup.select(selector)) for name,selector in {
        'canonical': 'head link[rel="canonical"][href]',
        'article': 'article[itemtype="https://schema.org/Article"]',
        'headline': 'article > meta[itemprop="headline"][content]',
        'entity': 'article > meta[itemprop="mainEntityOfPage"][itemid]',
        'body': 'article .tdb_single_content > .tdb-block-inner',
    }.items()}
    # Literal public source locators are evidence, never accepted aliases.
    # Keep only bounded native article declarations; no guessed destinations.
    facts['literal_native_link_declarations'] = {name:[str(n[attr]) for n in soup.select(selector)[:4]
        if len(str(n[attr]))<=4096 and _literal_identity(str(n[attr])) is not None]
        for name,selector,attr in (
            ('canonical','head link[rel="canonical"][href]','href'),
            ('main_entity','article > meta[itemprop="mainEntityOfPage"][itemid]','itemid'))}
    facts['declared_links_are_not_accepted_aliases'] = True
    try:
        _, body = _native_frame(soup, page_url=page_url)
    except NewsError as error:
        facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
        return facts
    facts['native_frame_verified'] = True
    known = _WRITTEN_HTML_TAGS | {'img', 'figure', 'figcaption', 'script', 'iframe', 'style',
        'form', 'input', 'video', 'audio', 'noscript', 'svg', 'picture', 'source', '#text'}
    counts, exceptions, walked, comments = Counter(), [], 0, 0
    for node in body.descendants:
        walked += 1
        if walked > 4096:
            break
        if isinstance(node, Comment):
            comments += 1
        tag = getattr(node, 'name', None)
        if not tag:
            continue
        safe_tag = tag if tag in known else '#other'
        counts[safe_tag] += 1
        try:
            _visible(node)
            visible = True
        except NewsError:
            visible = False
        if len(exceptions) < 24 and (tag not in _WRITTEN_HTML_TAGS or not visible):
            path, parent = [], node
            while parent is not body and parent is not None and len(path) < 8:
                path.append(parent.name if parent.name in known else '#other')
                parent = parent.parent
            shape = dict(tag=safe_tag, path=list(reversed(path)), path_truncated=parent is not body,
                visible=visible, text_characters=len(node.get_text(' ', strip=True)))
            if tag == 'figure':
                shape.update(images=len(node.find_all('img')), captions=len(node.find_all('figcaption')),
                    text_outside_caption=any(str(t).strip() and t.find_parent('figcaption') is None
                        for t in node.find_all(string=True)))
            exceptions.append(shape)
    facts.update(tag_counts=dict(counts), exceptional_nodes=exceptions, comments=comments,
        walked_nodes=min(walked,4096), walk_limit_reached=walked>4096)
    try:
        contentplatform_native_article(soup,page_url=page_url,adapter_contract_version=7)
    except NewsError as error:
        facts.update(current_native_error=error.code, failure_location=extraction_failure_location(error))
    else:
        facts['current_native_error'] = None
    return facts
