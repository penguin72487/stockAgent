"""Mega News native TagDiv article, using the shared HTTP and text walkers.

The WordPress template ID is not the article ID. Bind the literal canonical
and main-entity URL instead; retain the source's visible author spelling and
separate publication/modification clocks. No requests or endpoint inference.
"""
from __future__ import annotations

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
CONTENTPLATFORM_ORIGINAL_QUESTION = 'contentplatform_private_signed_native_geometry_original_v1'
CONTENTPLATFORM_ROOT_QUESTION = 'contentplatform_private_leading_nested_credit_root_original_v1'
CONTENTPLATFORM_IMAGE_QUESTION = 'contentplatform_private_first_written_credit_after_leading_image_original_v1'
CONTENTPLATFORM_EVIDENCE_QUESTIONS = {
    43: CONTENTPLATFORM_ORIGINAL_QUESTION,
    44: CONTENTPLATFORM_ROOT_QUESTION,
    45: CONTENTPLATFORM_IMAGE_QUESTION,
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


def _first_written_shape(metadata, body):
    """Bounded same-DOM evidence; the first paragraph is never skipped."""
    stack, images, credit, stopped, walked, comments = [(iter(body.contents), [])], [], None, None, 0, 0
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


def contentplatform_native_article(soup, *, page_url, adapter_contract_version=1):
    if adapter_contract_version not in {1, 2, 3}:
        raise ValueError('unreviewed Contentplatform adapter version')
    metadata, body = _native_header(soup, page_url=page_url)
    figures = []
    images = []
    for node in body.find_all(True):
        if node.name == 'figure':
            # Exclude this observed media/caption unit, never unknown prose.
            if (node.parent is not body or 'wp-caption' not in node.get('class', [])
                    or not node.find('img') or node.select('p, h1, h2, h3, table, blockquote, script, iframe')):
                raise NewsError('article_identity_not_verified')
            figures.append(node)
        elif node.name == 'img' and node.find_parent('figure') is None:
            parent = node.parent
            # The retained native v3 witness uses an empty h3 as an image
            # wrapper. Written headings, mixed prose and unknown media trees
            # must still fail instead of losing words from accepted text.
            media_parents = {'p', 'h3'} if adapter_contract_version >= 3 else {'p'}
            if (adapter_contract_version < 2 or parent.name not in media_parents or parent.parent is not body
                    or parent.get_text(strip=True) or len(parent.find_all('img', recursive=False)) != 1
                    or any(child.name != 'img' for child in parent.find_all(True))
                    or not any(re.fullmatch(r'wp-image-[1-9][0-9]*', name) for name in node.get('class', []))):
                raise NewsError('article_identity_not_verified')
            _visible(node)
            images.append(node)
        elif node.find_parent('figure') is None:
            if node.name not in _TEXT_TAGS:
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
            or source.embedded_article_layout not in {'contentplatform_native_article_v1', *CONTENTPLATFORM_ORIGINAL_LAYOUTS}
            or source.domains != ('www.contentplatform.info',)
            or source.authorization.get('kind') != 'personal_research'
            or source.authorization.get('evidence_url') != 'https://www.contentplatform.info/copyright/'
            or response.status != 200 or _literal_identity(response.url) is None
            or not re.search(r'(?:text/html|application/xhtml[+]xml)', response.headers.get('content-type', ''), re.I)):
        raise NewsError('private_contentplatform_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    _, body = _native_header(soup, page_url=response.url, include_failure_context=True)
    if body.select('script, iframe, form, input, video, audio'):
        raise NewsError('private_contentplatform_original_scope_not_verified')
    for node in body.find_all(True):
        _visible(node)
    return {'contract_version': 1, 'literal_native_identity_verified': True,
        'signed_reporter_and_explicit_clocks_verified': True,
        'body_geometry_accepted': False, 'article_accepted': False}
