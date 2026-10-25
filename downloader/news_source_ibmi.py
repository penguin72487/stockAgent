"""IBMI public source-metadata declarations, without body/rights admission.

The visible day, metadata-ingestion clock and first public availability are
different facts. Literal PageNO and double-slash views remain separate URLs;
an equal document ID/title is not an alias witness.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date
from itertools import islice
import re
from urllib.parse import parse_qsl, urlsplit

from bs4 import NavigableString


IBMI_METADATA_VERSION = 4
_SITE_NAME = '社團法人國家生技醫療產業策進會'
_MEDIA = ('工商時報', '經濟日報', '聯合報', '中國時報', '中央社', '鉅亨', 'MoneyDJ', '財訊快報',
          '中時電子報', '自立電子報', '時報資訊', '太平洋日報', '自由時報')
_CREDIT = re.compile(r'^[【\[（(]?\s*(?:資料來源|新聞來源|來源|出處|作者|記者|撰文|編譯|'
                     + '|'.join(re.escape(name) for name in _MEDIA) + ')')
_BRACKET_CREDIT = re.compile(r'^\s*(?:[0-9]{4}[/.-][0-9]{1,2}[/.-][0-9]{1,2}\s*)?'
                             r'[【\[][^】\]]{1,220}[】\]]'
                             r'(?:[ \t]*(?:19|20)[0-9]{2}[/.-][0-9]{1,2}[/.-][0-9]{1,2})?')
_PLAIN_CREDIT_LABEL = re.compile(r'^(?:資料來源|新聞來源|來源|出處|作者|記者|撰文|編譯)\s*[:：/／]\s*\S.{0,160}$')


def _native_node_visible(node, body):
    if isinstance(node, str) and type(node) is not NavigableString:
        return False  # Comments/declarations are not source-visible signatures.
    for parent in (node, *islice(node.parents, 64)):
        if getattr(parent, 'name', None):
            style = str(parent.get('style', ''))
            if (parent.has_attr('hidden') or parent.get('aria-hidden') == 'true'
                    or parent.name in {'script', 'style', 'template', 'noscript'}
                    or re.search(r'(?:^|;)\s*(?:display\s*:\s*none|visibility\s*:\s*hidden)\b', style, re.I)):
                return False
        if parent is body:
            return True
    return False


def _text_prefix(node, limit=320):
    """Read only enough inline text to inspect a bounded leading signature."""
    if isinstance(node, str):
        return str(node).strip()[:limit]
    parts, size = [], 0
    for text in node.strings:
        if not _native_node_visible(text, node):
            continue
        raw = str(text)
        if not parts:
            raw = raw.lstrip()
        part = raw[:max(0, limit - size)]
        if not part:
            continue
        parts.append(part)
        size += len(part)
        if size >= limit:
            break
    return ''.join(parts).strip()


def _credit_prefix(text):
    """A closing bracket bounds the signature, so following prose stays out."""
    match = _BRACKET_CREDIT.match(text)
    if not match:
        return None
    candidate = match.group().strip()
    if (_CREDIT.match(candidate)
            or (any(name in candidate for name in _MEDIA)
                and re.search(r'記者|編譯|撰文|作者', candidate))):
        return candidate
    return None


def ibmi_credit_declarations(candidates):
    """Classify saved native header strings without another article request.

    This operates on bounded observations, not inferred names, DOM identity,
    ownership or authorization. Source labels remain separate: e.g. 中時電子報
    and 時報資訊 are never collapsed into another publisher or a URL alias.
    """
    if (not isinstance(candidates, list) or len(candidates) > 24
            or any(not isinstance(text, str) or not 0 < len(text) <= 240 for text in candidates)):
        raise ValueError('bounded_native_credit_candidates_required')
    selected = list(dict.fromkeys(text for text in candidates if _credit_prefix(text)
                                 or _PLAIN_CREDIT_LABEL.fullmatch(text)))
    media = [name for name in _MEDIA if any(name in text for text in selected)]
    return {'credit_candidates': selected, 'declared_media_names': media,
        'syndication_status': 'declared_third_party_credit' if media else 'unresolved',
        'credit_declarations_contract_version': IBMI_METADATA_VERSION,
        'original_authorship_verified': False, 'body_authorization_granted': False}


def _inline_credit_metadata(body, soup):
    """Observe only native signature prefixes, never whole short paragraphs."""
    candidates, shapes, unresolved_headers = [], [], []
    for node in islice(body.children, 16):
        name = getattr(node, 'name', None)
        if not _native_node_visible(node, body):
            continue
        prefix = _text_prefix(node)
        if not prefix:
            continue
        credit = _credit_prefix(prefix)
        if not credit and len(shapes) < 2:
            bracket = _BRACKET_CREDIT.match(prefix)
            header = bracket.group().strip() if bracket else prefix
            if (len(header) <= 128 and re.search(r'記\s*者|作者|編譯|撰文|資料來源|時報|日報|通訊社', header)
                    and (bracket or re.match(r'^(?:[0-9]{4}[/.-][0-9]{1,2}[/.-][0-9]{1,2}|記者|作者|編譯|撰文|資料來源)', header))):
                unresolved_headers.append(header)
        shapes.append({'tag': name or '#text',
            'classes': node.get('class') if name else None,
            'prefix_characters_observed': len(prefix), 'signature_prefix_found': credit is not None})
        if credit and credit not in candidates and len(candidates) < 12:
            candidates.append(credit)
    descriptions = soup.select('meta[property="og:description"]')
    description_credit = (_credit_prefix(str(descriptions[0].get('content', ''))[:320])
                          if len(descriptions) == 1 else None)
    return candidates, {'direct_child_shapes': shapes,
        'unrecognized_native_header_candidates': unresolved_headers,
        'description_signature_prefix': description_credit,
        'description_signature_is_visible_byline': bool(description_credit and description_credit in candidates)}


def _literal_article(url):
    try:
        parts = urlsplit(url)
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        values = dict(pairs)
        return (parts.scheme == 'https' and parts.netloc == 'ibmi.taiwan-healthcare.org'
            and parts.path in {'/zh/news_detail.php', '/zh//news_detail.php'}
            and not parts.fragment and len(pairs) == len(values) == 3
            and set(values) == {'REFDOCID', 'REFDOCTYPID', 'PageNO'}
            and all(re.fullmatch(r'[A-Za-z0-9]+', values[key]) for key in ('REFDOCID', 'REFDOCTYPID'))
            and bool(re.fullmatch(r'[0-9]+', values['PageNO'])))
    except (TypeError, ValueError):
        return False


def ibmi_validation_metadata(soup, *, page_url):
    """Return bounded native evidence; never return body prose or infer a time.

    In particular, unknown/third-party credits cannot grant archive extraction.
    A recognized media name is a declared credit, not a verified person or an
    original-publisher URL. Links are observations, not fetch instructions.
    """
    result = {'metadata_contract_version': IBMI_METADATA_VERSION,
        'identity_status': 'unverified', 'title_raw': None,
        'publication_date_raw': None, 'publication_precision': None,
        'publication_day_valid': False, 'credit_candidates': [],
        'syndication_status': 'unresolved', 'declared_media_names': [],
        'article_links': [], 'article_durably_accepted': False,
        'body_text_retained': False, 'historical_first_publication_verified': False}
    if not _literal_article(page_url):
        result['failure'] = 'unsupported_literal_article_url'
        return result
    scopes = soup.select('.center-left')
    bodies = soup.select('.center-left-value')
    result.update(scope_count=len(scopes), body_container_count=len(bodies))
    if (len(scopes) != 1 or len(bodies) != 1
            or not any(parent is scopes[0] for parent in bodies[0].parents)):
        result['failure'] = 'native_container_not_unique'
        return result
    scope, body = scopes[0], bodies[0]
    titles = scope.select(':scope > .center-left-title')
    dates = titles[0].select(':scope > .center-left-date') if len(titles) == 1 else []
    declarations = soup.select('meta[property="og:url"]')
    if (len(titles) != 1 or len(dates) != 1 or len(declarations) != 1
            or declarations[0].get('content') != page_url):
        result['failure'] = 'native_identity_declaration_not_verified'
        return result
    title_node = deepcopy(titles[0])
    title_node.select_one(':scope > .center-left-date').decompose()
    title = title_node.get_text(' ', strip=True)
    og_titles = soup.select('meta[property="og:title"]')
    if (not title or len(title) > 1000 or len(og_titles) != 1
            or og_titles[0].get('content') != title + '｜' + _SITE_NAME):
        result['failure'] = 'native_title_declaration_not_verified'
        return result
    result.update(identity_status='literal_source_declaration_matches', title_raw=title)
    raw_day = dates[0].get_text(' ', strip=True)
    result['publication_date_raw'] = raw_day[:80]
    if re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', raw_day):
        try:
            date.fromisoformat(raw_day)
        except ValueError:
            pass
        else:
            result.update(publication_day_valid=True, publication_precision='day')
    # Inspect bounded paragraph-level credit candidates, without duplicating
    # nested span fragments or retaining ordinary short article paragraphs.
    candidates, inline_metadata = _inline_credit_metadata(body, soup)
    for node in body.find_all(['p', 'div'], limit=128):
        if node.find(['p', 'div']) is not None or not _native_node_visible(node, body):
            continue
        text = _text_prefix(node)
        credit = _credit_prefix(text)
        if credit is None and 0 < len(text) <= 240 and _PLAIN_CREDIT_LABEL.fullmatch(text):
            credit = text
        if credit and credit not in candidates and len(candidates) < 12:
            candidates.append(credit)
        if len(candidates) == 12:
            break
    result['credit_candidates'] = candidates
    result['native_credit_structure'] = inline_metadata
    result.update(ibmi_credit_declarations(candidates))
    result['body_characters'] = len(body.get_text(' ', strip=True))
    result['article_links'] = [{'href': str(n.get('href', ''))[:2048],
                               'label': n.get_text(' ', strip=True)[:120]}
                              for n in body.select('a[href]')[:24]]
    return result
