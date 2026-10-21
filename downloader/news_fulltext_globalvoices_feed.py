"""Separate a verified GV RSS envelope from its article, without any I/O.

The feed's publication notice and contributor footer are provenance, not prose.
Unknown envelopes keep their HTML fallback; they never become short-body proof.
"""
from __future__ import annotations

import json
import re
from urllib.parse import urlsplit

from bs4 import NavigableString, Tag

from downloader.news_fulltext_policy import NewsError, _plain_text_node, normalize_url
from downloader.news_fulltext_public_originals import _visible


def _children(node):
    children = []
    for child in node.children:
        if isinstance(child, Tag):
            children.append(child)
        elif type(child) is not NavigableString or str(child).strip():
            raise NewsError('invalid_bulk_post_fields')
    return children


def _text(node):
    return ' '.join(node.stripped_strings)


def _source_link(node, *, profile=False, post_id=False):
    href = node.get('href')
    try:
        normalize_url(href)  # Validate; retain the publisher's literal URL.
        parts = urlsplit(href)
    except (NewsError, ValueError):
        raise NewsError('invalid_bulk_post_fields') from None
    host = parts.hostname or ''
    # Historical contributor hrefs are literal source metadata, not aliases
    # to request. Retain the old translated/subproject paths without a fetch.
    roots = ('globalvoices.org', 'globalvoicesonline.org')
    native_post_id = post_id and parts.path == '/' and re.fullmatch(r'p=[1-9][0-9]*', parts.query)
    if (parts.scheme != 'https' or parts.netloc != host or parts.query and not native_post_id or parts.fragment
            or not any(host == root or host.endswith('.' + root) for root in roots)):
        raise NewsError('invalid_bulk_post_fields')
    if profile and not re.fullmatch(r'/(?:blog/|hant/)?author/[^/]+/?', parts.path):
        raise NewsError('invalid_bulk_post_fields')
    if not _text(node):
        raise NewsError('article_attribution_not_verified')
    return {'text': _text(node), 'url': href}


def _tagline(node):
    """The observed native p > big.tagline > em, retained as article prose."""
    children = _children(node)
    if (node.name != 'p' or len(children) != 1 or children[0].name != 'big'
            or children[0].get('class') != ['tagline']):
        return False
    inner = _children(children[0])
    if len(inner) != 1 or inner[0].name != 'em' or inner[0].find(True) or not _text(inner[0]):
        return False
    for part in (node, children[0], inner[0]):
        _visible(part)
    return True


def globalvoices_feed_body(fragment, source, *, url, authors, body_contract_version=3):
    """Return prose, literal envelope metadata, and verified-short-prose status.

    This is only the registered publisher's explicit content:encoded field.
    Never borrow its description, follow credit links, infer a clock or assign
    translators to an original-author role. The raw response stays untouched.
    """
    # One bounded traversal, not two full CSS scans of a potentially long post.
    markers = fragment.find_all(class_=['originally-published', 'gv-rss-footer'])
    notices = [node for node in markers if 'originally-published' in node.get('class', [])]
    footers = [node for node in markers if 'gv-rss-footer' in node.get('class', [])]
    if not notices and not footers:
        return _plain_text_node(fragment, source.exclude_selectors, parser=source.html_parser), None, False
    if len(notices) > 1 or len(footers) != 1:
        raise NewsError('invalid_bulk_post_fields')
    root, footer = fragment.body or fragment, footers[0]
    children = _children(root)
    if not children or children[-1] is not footer or footer.name != 'div':
        raise NewsError('invalid_bulk_post_fields')
    for node in [footer, *footer.find_all(True), *notices]:
        _visible(node)
    inner = _children(footer)
    if len(inner) != 1 or inner[0].name != 'strong':
        raise NewsError('invalid_bulk_post_fields')
    sections = _children(inner[0])
    if not 1 <= len(sections) <= 2 or sections[0].get('class') != ['text-credits-container']:
        raise NewsError('invalid_bulk_post_fields')
    groups = _children(sections[0])
    if not 1 <= len(groups) <= 8:
        raise NewsError('invalid_bulk_post_fields')
    credits = []
    for group in groups:
        parts = _children(group)
        if (group.name != 'div' or group.get('class') != ['text-credits-section'] or len(parts) != 2
                or parts[0].name != 'span' or parts[0].get('class') != ['credit-label']
                or parts[1].name != 'a' or parts[1].get('class') != ['user-link']
                or parts[0].find(True) or parts[1].find(True)):
            raise NewsError('invalid_bulk_post_fields')
        label = _text(parts[0])
        language_role = body_contract_version >= 3 and re.fullmatch(
            r'(?:Written|Translated) \([^()\r\n]{1,64}\) by', label)
        if (not 0 < len(label) <= 96 or not (label in {'Written by', 'Translated by'} or language_role
                or label.startswith(('作者 (', '譯者 (')) and label.endswith(')'))):
            raise NewsError('article_attribution_not_verified')
        link = _source_link(parts[1], profile=True)
        credits.append({'role_label': label, 'name': link['text'], 'profile_url': link['url']})
    names = {credit['name'] for credit in credits}
    if not authors or not all(' '.join(author.split()) in names for author in authors):
        raise NewsError('article_attribution_not_verified')
    original = None
    if len(sections) == 2:
        links = _children(sections[1])
        if (sections[1].name != 'span' or sections[1].get('class') != ['source-link']
                or len(links) != 1 or links[0].name != 'a' or links[0].find(True)):
            raise NewsError('invalid_bulk_post_fields')
        # Some native translated credits retain a WordPress ?p=ID source
        # permalink. It is metadata only, never an alias or a request target.
        original = _source_link(links[0], post_id=body_contract_version >= 3)
    notice = None
    if notices:
        node = notices[0]
        parts = _children(node)
        first_notice = children[0] is node
        tagline_notice = (body_contract_version >= 3 and len(children) > 1 and children[1] is node
                          and _tagline(children[0]))
        if not (first_notice or tagline_notice) or node.name != 'p' or len(parts) != 1 or parts[0].name != 'small':
            raise NewsError('invalid_bulk_post_fields')
        links = parts[0].find_all('a')
        if (len(links) != 1 or links[0].find(True) or _text(links[0]) != 'Global Voices'
                or _text(node) != 'Originally published on Global Voices'
                or normalize_url(links[0].get('href')) != url
                or [n.name for n in parts[0].find_all(True)] != ['a']):
            raise NewsError('invalid_bulk_post_fields')
        _visible(links[0])
        notice = {'text': _text(node), 'url': links[0]['href']}
    # Mutate only this temporary fragment after its entire envelope is checked.
    for node in notices + footers:
        node.decompose()
    body = _plain_text_node(fragment, source.exclude_selectors, parser=source.html_parser)
    short = False
    if 0 < len(body) < 100 and any(c.isalnum() for c in body):
        forbidden = ['img', 'script', 'style', 'iframe', 'object', 'embed', 'audio', 'video',
                     'svg', 'math', 'form', 'input', 'button', 'textarea', 'noscript', 'template']
        if not fragment.find(forbidden):
            for node in fragment.find_all(True):
                _visible(node)
            short = True
    metadata = {'contract_version': body_contract_version, 'originally_published_notice': notice,
                'credits': credits, 'original_source_link': original,
                'verification_basis': 'native_content_encoded_envelope; feed_creator_matches_visible_credits'}
    return body, json.dumps(metadata, ensure_ascii=False, sort_keys=True), short
