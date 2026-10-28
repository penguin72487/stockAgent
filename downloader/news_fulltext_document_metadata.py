"""Bounded public identity declarations, without article prose or admission."""
from __future__ import annotations

import hashlib
from itertools import islice
import json
import re
from urllib.parse import urlsplit


def public_original_link_shape(soup):
    """Literal original-link declarations are discovery, never identity proof."""
    labels = {'原文', '原文連結', '閱讀原文', '查看原文', '看原文', '完整原文',
              '原始文章', '原始新聞', 'originalarticle', 'readoriginal'}
    nodes = soup.find_all(['a', 'link'], href=True, limit=513)
    candidates, matched = [], 0
    for node in nodes[:512]:
        relations = sorted(set(node.get('rel', ())) & {'cite', 'via'})
        pieces, length = [], 0
        for text in node.stripped_strings:
            length += len(text)
            if length > 128:
                break
            pieces.append(text)
        label_matches = length <= 128 and re.sub(r'\s+', '', ''.join(pieces)).casefold() in labels
        if not relations and not label_matches:
            continue
        href = node.get('href')
        if (not isinstance(href, str) or not 0 < len(href) <= 2048
                or any(ord(char) <= 32 or ord(char) == 127 for char in href)):
            continue
        try:
            parts = urlsplit(href)
            if (parts.scheme not in {'http', 'https'} or not parts.hostname
                    or parts.username is not None or parts.password is not None):
                continue
            parts.port
        except ValueError:
            continue
        matched += 1
        if len(candidates) < 8:
            candidates.append({'href': href, 'node': node.name,
                'relations': relations, 'explicit_original_label': label_matches})
    return {'contract_version': 1, 'candidates': candidates,
        'nodes_examined': min(len(nodes), 512), 'node_limit_reached': len(nodes) > 512,
        'candidate_limit_reached': matched > 8,
        'sampling_limits': {'link_nodes': 512, 'candidates': 8, 'label_characters': 128,
                            'href_characters': 2048},
        'scope': 'literal_absolute_HTTP_link_declarations_only_not_original_identity',
        'relative_links_resolved': False, 'body_text_exported': False,
        'automatic_requests': 0, 'original_source_alias_admitted': False}


def _scalar(value):
    if isinstance(value, str):
        if len(value) <= 2048:
            return value
        return {'value_omitted': True, 'characters': len(value),
                'sha256': hashlib.sha256(value.encode()).hexdigest()}
    return value if value is None or type(value) in {bool, int} else {'type': type(value).__name__}


def public_document_identity_shape(soup):
    """Project declarations from one DOM; discard descriptions/articleBody."""
    def entity(value, depth=0):
        if depth > 2:
            return {'nested_value_omitted': True, 'type': type(value).__name__}
        if isinstance(value, list):
            return [entity(item, depth + 1) for item in value[:8]]
        if isinstance(value, dict):
            return {key: _scalar(value[key]) for key in ('@type', '@id', 'name', 'url') if key in value}
        return _scalar(value)

    metas = []
    permitted = {'og:url', 'og:title', 'og:site_name', 'article:published_time',
                 'article:modified_time', 'author'}
    for node in soup.find_all('meta'):
        field = node.get('property') or node.get('name')
        if field in permitted:
            metas.append({'field': field, 'value': _scalar(node.get('content'))})
            if len(metas) == 16:
                break
    articles, errors, examined = [], [], 0
    for node in islice(soup.find_all('script', type='application/ld+json'), 8):
        if examined >= 256 or len(articles) >= 8:
            break
        try:
            stack = [(json.loads(node.get_text()), 0)]
        except (ValueError, TypeError, RecursionError):
            errors.append('invalid_jsonld')
            continue
        while stack and examined < 256 and len(articles) < 8:
            value, depth = stack.pop()
            examined += 1
            if depth > 8:
                continue
            if isinstance(value, list):
                stack.extend((child, depth + 1) for child in reversed(value[:256]))
            elif isinstance(value, dict):
                kinds = value.get('@type')
                kinds = kinds if isinstance(kinds, list) else [kinds]
                if any(kind in ('Article', 'NewsArticle', 'BlogPosting') for kind in kinds):
                    articles.append({key: entity(value[key]) for key in (
                        '@type', '@id', 'url', 'mainEntityOfPage', 'headline', 'author',
                        'publisher', 'datePublished', 'dateModified', 'isBasedOn',
                        'isAccessibleForFree') if key in value})
                if '@graph' in value:
                    stack.append((value['@graph'], depth + 1))
    return {'contract_version': 2,
        'html_title': _scalar(soup.title.get_text(' ', strip=True)) if soup.title else None,
        'canonical_links': [_scalar(node.get('href'))
                            for node in islice(soup.select('link[rel~=canonical][href]'), 8)],
        'head_metadata': metas, 'declared_articles': articles, 'jsonld_errors': errors,
        'jsonld_nodes_examined': examined,
        'original_link_declarations': public_original_link_shape(soup),
        'metadata_sampling_limits': {'head_fields': 16, 'canonical_links': 8,
            'jsonld_scripts': 8, 'jsonld_nodes': 256, 'jsonld_depth': 8, 'articles': 8,
            'entity_list_items': 8, 'scalar_characters': 2048},
        'raw_or_body_retained': False, 'article_identity_verified': False,
        'original_source_alias_admitted': False, 'historical_publication_verified': False}
