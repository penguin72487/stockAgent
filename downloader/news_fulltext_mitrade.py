"""Observed Mitrade native analysis pages; pure parsing, never HTTP/discovery.

Bind the literal locale/path to visible prose and the publisher's declarations.
Reviewed native profile labels are not independently verified people; current
schema clocks are not historical observation times or an article reuse license.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


MITRADE_ORIGIN = 'https://www.mitrade.com'
MITRADE_PATH_PATTERN = (
    r'^/(?:cn/insights/shares-analysis/more|zh/insights/(?:'
    r'shares-analysis/(?:more|weekly|outlook-of-month)|commodity-analysis/(?:oil|xau)|'
    r'crypto-analysis/eth|forex-analysis/(?:eur|more)|more-analysis/(?:industry-analysis|more)))'
    r'/[A-Za-z0-9_-]{1,128}$'
)
MITRADE_BODY_SELECTOR = ':scope > .mitrade-native-body'
_TITLE = ':scope > .article-content__articleTitle__fbEXV'
_CONTENT = ':scope > .article-content__articleDetailCon__kmMNZ'
_PROFILES = {
    ('cn', 'Insights'): 'insights',
    ('zh', '投資指南針'): 'compass',
    ('zh', '財富進化論'): 'wealth',
}
_V2_PROFILES = {('zh', 'Mitrade 分析師'): 'mitrade-editor'}
_V4_PROFILES = {('zh', 'Insights'): 'insights'}
_V5_PROFILES = {('zh', 'Alison Ho'): 'alison-ho', ('zh', 'Tony Chou'): 'tony'}
_V6_PROFILES = {('zh', '投資-槓把子'): 'Invest'}
_V7_PROFILES = {('zh', 'Mitrade'): 'mitrade-team'}
_AI_ASSISTED_NOTICE = '以上內容在AI的輔助下完成，並經過編輯校對。'
# Filled only by the separately reviewed production scope transition. Unlike
# a parser-only migration, this requires an exact old/new capture contract.
MITRADE_V2_SCOPE_REPAIR_IDENTITY = (
    'deed2af6691c328ab37673ea3a11bb905b68079b598d07e0a7102f58bb7b2ce0',
    '9a4b7becae07fa7250e6d675f6a2c733d6be22945edbe8b7ff9b89526772b0d1',
)
MITRADE_V4_SCOPE_REPAIR_IDENTITY = (
    '8bd2c79f1fc94e733f3abc2fed29f240bb3a5de71c6009d5de442012f312e19a',
    'bcfc5964e85766b655a05b7db3477996b699d620316b3e3c6a37f78eb2fc75ad',
)
MITRADE_V5_SCOPE_REPAIR_IDENTITY = (
    'bcfc5964e85766b655a05b7db3477996b699d620316b3e3c6a37f78eb2fc75ad',
    'c17d12406749e071a9342f9d41a60a1fc871d08f7b76f09aa2512b448a04ce3c',
)
# The reviewed v4 -> v5 transition changes native attribution scope, not HTTP
# permission or request geometry. Only the complete pinned new capture above
# may reuse this old *measurement* identity. Body/provenance authorization
# always uses the actual new policy, never this compatibility fingerprint.
MITRADE_V5_PRIOR_HTTP_AUTHORIZATION_FINGERPRINT = (
    'da2354cce2997ad7c01aef71331c9f997fdea59c19887908135edc1aa54a491d'
)
# The reviewed v6 scope is a literal, case-sensitive publisher pen-name link.
# Retry only the two missing originals actually inspected, not every exclusion.
MITRADE_V6_SCOPE_REPAIR_IDENTITY = (
    'c17d12406749e071a9342f9d41a60a1fc871d08f7b76f09aa2512b448a04ce3c',
    '05e38e4d9e9154e989259437850310750b0a7f29e2ca5fde393d6541b545a966',
)
MITRADE_V6_REPAIR_COHORT = frozenset({
    MITRADE_ORIGIN + '/zh/insights/shares-analysis/more/20250922S04',
    MITRADE_ORIGIN + '/zh/insights/shares-analysis/more/20250922S01',
})
# v5 already measures HTTP under v4's identity. Inheriting v5's *capture*
# identity here would silently discard the saved measurements a second time.
MITRADE_V6_PRIOR_HTTP_MEASUREMENT_IDENTITY = (
    'bcfc5964e85766b655a05b7db3477996b699d620316b3e3c6a37f78eb2fc75ad',
    'da2354cce2997ad7c01aef71331c9f997fdea59c19887908135edc1aa54a491d',
)
# Pinned after metadata-only native company/reviewer and primary policy review.
# The one inspected failed head is not authority to retry every old exclusion.
MITRADE_V7_SCOPE_REPAIR_IDENTITY = (
    '05e38e4d9e9154e989259437850310750b0a7f29e2ca5fde393d6541b545a966',
    'bf698a7f7c501544c998515ad2a8ef6c7ed57262092258f6b62120ad1aa3b5c3',
)
MITRADE_V7_REPAIR_COHORT = frozenset({
    MITRADE_ORIGIN + '/zh/insights/shares-analysis/more/3231-gen-20251208',
})
MITRADE_V7_PRIOR_HTTP_MEASUREMENT_IDENTITY = MITRADE_V6_PRIOR_HTTP_MEASUREMENT_IDENTITY
_BODY_TAGS = frozenset({'div', 'p', 'span', 'a', 'br', 'img', 'strong', 'b', 'em', 'i',
    'u', 's', 'sub', 'sup', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li',
    'blockquote', 'table', 'caption', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td'})
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')


def mitrade_article_id(url, *, requested=False):
    """Keep the entire literal locale/path identity, never a date or an alias."""
    if not isinstance(url, str) or type(requested) is not bool:
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.mitrade.com'
            or parts.query or parts.fragment or not re.fullmatch(MITRADE_PATH_PATTERN, parts.path)):
        return None
    return parts.path


def mitrade_validation_metadata(soup, *, page_url, include_editorial=False):
    """Bounded declarations from the same received DOM, never body retention.

    Unknown native bylines/layouts can be repaired from source evidence without
    repeating the publisher request. These facts grant no author/body scope.
    """
    def values(selector, attribute=None, *, url=False):
        nodes = soup.select(selector)
        result = []
        for node in nodes[:8]:
            value = node.get(attribute) if attribute else node.get_text(' ', strip=True)
            if not isinstance(value, str):
                result.append({'type': type(value).__name__})
                continue
            display = value
            if url:
                try:
                    parts = urlsplit(value)
                    display = (parts._replace(query='', fragment='').geturl()
                        if parts.scheme == 'https' and parts.netloc == 'www.mitrade.com'
                        and not parts.username and not parts.password else None)
                except ValueError:
                    display = None
            result.append({'value': display[:512] if display is not None else None,
                'characters': len(value), 'sha256': hashlib.sha256(value.encode()).hexdigest(),
                'truncated': len(value) > 512})
        return {'node_count': len(nodes), 'truncated': len(nodes) > 8, 'values': result}

    credits = '#articleDetail > '+_TITLE.removeprefix(':scope > ')+' .article-content__newsAuthor__k-zC2'
    native = soup.select('#articleDetail > #articleCon > '+_CONTENT.removeprefix(':scope > ')+' > .article-content__contextC__QMEJJ')
    structure = []
    for node in native[:8]:
        elements = node.find_all(True, limit=4097)
        counts = {}
        for element in elements[:4096]:
            counts[element.name] = counts.get(element.name, 0) + 1
        structure.append({'sampled_elements': min(4096, len(elements)), 'truncated': len(elements) > 4096,
            'tag_counts': dict(sorted(counts.items())[:64]), 'tag_names_truncated': len(counts) > 64})
    result = {'contract_version': 1, 'literal_url_recognized': mitrade_article_id(page_url) is not None,
        'article_scope_count': len(soup.select('#articleDetail')), 'global_h1_count': len(soup.select('h1')),
        'canonical_urls': values('head > link[rel=canonical]', 'href', url=True),
        'og_urls': values('head > meta[property="og:url"],head > meta[name="og:url"]', 'content', url=True),
        'native_credits': values(credits), 'native_profiles': values(credits, 'href', url=True),
        'native_body_count': len(native), 'native_body_structures': structure,
        'labels_are_observations_not_verified_attribution': True,
        'body_text_retained': False, 'raw_response_retained': False,
        'article_identity_or_scope_granted': False, 'additional_requests': 0}
    if include_editorial:
        # Explicit operator diagnosis only. Keep the automatic failure ABI and
        # its persistent sampling budgets unchanged. This never grants scope.
        result['editorial_structure'] = _editorial_structure(soup)
    return result


def _editorial_structure(soup):
    """Bounded native credit/schema/notice shapes, without unverified prose.

    The caller already owns the received DOM. Do not fetch linked profiles,
    expose arbitrary schema strings (articleBody/description in particular),
    or confuse a declared reviewer/company with a verified human author.
    """
    def scalar(value, limit=160):
        return value[:limit] if isinstance(value, str) else None

    def profile_url(value):
        if not isinstance(value, str) or len(value) > 512:
            return None
        try:
            parts = urlsplit(value)
        except ValueError:
            return None
        if (parts.scheme != 'https' or parts.netloc != 'www.mitrade.com'
                or not re.fullmatch(r'/(?:zh|cn)/insights/profile/[A-Za-z0-9_-]{1,128}', parts.path)):
            return None
        return MITRADE_ORIGIN + parts.path

    def shape(node):
        return {'tag': node.name, 'id': scalar(node.get('id'), 128),
            'classes': [scalar(value, 128) for value in node.get('class', [])[:8]],
            'child_tags': [child.name for child in node.find_all(recursive=False, limit=33)[:32]]}

    def sibling_shape(node):
        text = node.get_text(' ', strip=True)
        return {**shape(node), 'characters': len(text),
            'text_sha256': hashlib.sha256(text.encode()).hexdigest(),
            'matches_reviewed_AI_notice': text == _AI_ASSISTED_NOTICE}

    headers = soup.select('#articleDetail > ' + _TITLE.removeprefix(':scope > '))
    header_shapes, links = [], []
    for header in headers[:2]:
        nodes = header.find_all(True, limit=97)
        header_shapes.append({'elements': [shape(node) for node in nodes[:96]],
                              'truncated': len(nodes) > 96})
        for node in header.find_all('a', href=True, limit=33)[:32]:
            href = profile_url(node.get('href'))
            if href:
                links.append({**shape(node), 'label': scalar(node.get_text(' ', strip=True)),
                    'profile_url': href, 'parent': shape(node.parent)})

    bodies = soup.select('#articleDetail > #articleCon')
    siblings = []
    for con in bodies[:2]:
        nodes = con.find_all(recursive=False, limit=33)
        siblings.append({'children': [sibling_shape(node) for node in nodes[:32]],
                         'truncated': len(nodes) > 32})

    declarations = []
    scripts = soup.find_all('script', type='application/ld+json', limit=5)
    for script in scripts[:4]:
        text = script.get_text()
        entry = {'characters': len(text), 'oversized': len(text) > 1_048_576}
        if not entry['oversized']:
            try:
                declared = json.loads(text)
            except (ValueError, TypeError, RecursionError):
                entry['parse_error'] = True
            else:
                entry['object'] = isinstance(declared, dict)
                if isinstance(declared, dict):
                    entry['keys'] = [scalar(key) for key in list(declared)[:64]]
                    entry['keys_truncated'] = len(declared) > 64
                    entry['type'] = scalar(declared.get('@type'))
                    entry['free_boolean'] = (declared.get('isAccessibleForFree')
                        if type(declared.get('isAccessibleForFree')) is bool else None)
                    for field in ('author', 'reviewedBy'):
                        value = declared.get(field)
                        items = value if isinstance(value, list) else [value]
                        entry[field] = [{
                            'type': scalar(item.get('@type')), 'name': scalar(item.get('name')),
                            'profile_url': profile_url(item.get('url')),
                        } for item in items[:8] if isinstance(item, dict)]
                    for field in ('datePublished', 'dateModified'):
                        value = declared.get(field)
                        entry[field] = value if isinstance(value, str) and _CLOCK.fullmatch(value) else None
        declarations.append(entry)
    return {'contract_version': 1, 'header_count': len(headers), 'headers': header_shapes,
        'native_profile_links': links[:16], 'body_containers': siblings,
        'jsonld': declarations, 'scripts_truncated': len(scripts) > 4,
        'body_or_notice_text_retained': False, 'attribution_verified': False,
        'scope_granted': False, 'additional_requests': 0}


def _native_visible(node):
    _visible(node)
    for parent in [node, *node.parents]:
        if getattr(parent, 'attrs', None) and set(parent.get('class', [])).intersection(
                {'hidden', 'd-none', 'sr-only', 'screen-reader-text'}):
            raise NewsError('article_identity_not_verified')


def _visible_tree(node, *, table_columns=False):
    for element in [node, *node.find_all(True)]:
        if element.name not in _BODY_TAGS and not (table_columns and element.name in {'colgroup', 'col'}):
            raise NewsError('article_identity_not_verified')
        if element.name in {'colgroup', 'col'}:
            parent = 'table' if element.name == 'colgroup' else 'colgroup'
            if (element.parent.name != parent or (element.name == 'colgroup'
                    and (any(child.name != 'col' for child in element.find_all(recursive=False))
                         or element.get_text(strip=True)))):
                raise NewsError('article_identity_not_verified')
        _native_visible(element)


def mitrade_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Extract one witnessed native body; do not fall back to descriptions."""
    if type(adapter_contract_version) is not int or adapter_contract_version not in {1, 2, 3, 4, 5, 6, 7}:
        raise ValueError('unsupported Mitrade native adapter version')
    article_id = mitrade_article_id(page_url)
    if article_id is None:
        raise NewsError('article_identity_not_verified')
    locale = article_id.split('/')[1]
    scope = _one(soup, '#articleDetail')
    header = _one(scope, _TITLE)
    heading = _one(header, ':scope > h1.article-content__title__5GvQ6')
    title = heading.get_text(' ', strip=True)
    if (not title or len(soup.select('h1')) != 1
            or _one(soup, 'head > title').get_text(' ', strip=True) != title
            or _one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'):
        raise NewsError('article_identity_not_verified')
    for selector, value in [('head > meta[property="og:title"]', title),
                            ('head > meta[property="og:url"]', page_url),
                            ('head > meta[name="og:url"]', page_url)]:
        if _one(soup, selector).get('content') != value:
            raise NewsError('article_identity_not_verified')
    _native_visible(heading)
    script = _one(soup, 'script[type="application/ld+json"]')
    try:
        if len(script.get_text()) > 1_048_576:
            raise ValueError('oversized article declaration')
        declaration = json.loads(script.get_text())
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    if not isinstance(declaration, dict):
        raise NewsError('article_identity_not_verified')
    if declaration.get('isAccessibleForFree') is False:
        raise NewsError('paywall_or_login')
    publisher = declaration.get('publisher')
    if (declaration.get('@context') != 'https://schema.org'
            or declaration.get('@type') != 'NewsArticle'
            or declaration.get('headline') != title or declaration.get('url') != page_url
            or declaration.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or declaration.get('isAccessibleForFree') is not True
            or declaration.get('articleBody')
            or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
            or publisher.get('name') != ('投资慧眼' if locale == 'cn' else '投資慧眼')
            or publisher.get('logo') != {'@type': 'ImageObject',
                'url': MITRADE_ORIGIN + f'/insights/image/logo/logo_top_{locale}.png'}):
        raise NewsError('article_identity_not_verified')
    author = declaration.get('author')
    credit = _one(header, ':scope > .article-content__wrap__FGxnV > .article-content__flex1__W6hv6 > div > a.article-content__newsAuthor__k-zC2',
                  'article_attribution_not_verified')
    label = _one(credit, ':scope > span', 'article_attribution_not_verified')
    signature = credit.get_text(' ', strip=True)
    if (not isinstance(author, dict) or author.get('@type') != 'Person'
            or label.get_text(' ', strip=True) not in {'作者:', '作者 :'}
            or not isinstance(author.get('name'), str)
            or signature != label.get_text(' ', strip=True) + ' ' + author['name']):
        raise NewsError('article_attribution_not_verified')
    slug = _PROFILES.get((locale, author['name']))
    if slug is None and adapter_contract_version >= 2:
        slug = _V2_PROFILES.get((locale, author['name']))
    if slug is None and adapter_contract_version >= 4:
        slug = _V4_PROFILES.get((locale, author['name']))
    if slug is None and adapter_contract_version >= 5:
        slug = _V5_PROFILES.get((locale, author['name']))
    if slug is None and adapter_contract_version >= 6:
        slug = _V6_PROFILES.get((locale, author['name']))
    if slug is None and adapter_contract_version >= 7:
        slug = _V7_PROFILES.get((locale, author['name']))
    if slug is None:
        raise NewsError('article_rights_or_scope_excluded')
    profile = MITRADE_ORIGIN + f'/{locale}/insights/profile/{slug}'
    if author.get('url') != profile or credit.get('href') != profile:
        raise NewsError('article_attribution_not_verified')
    _native_visible(credit)
    corporate = (locale, author['name']) in _V7_PROFILES
    reviewers = []
    if adapter_contract_version >= 7:
        # The publisher labels its company as schema:Person. Keep that literal
        # declaration separately; neither author nor reviewer is a verified
        # human, and a visible review is not an independent fact-check.
        boxes = header.select('.article-content__reviewer__vvt-C')
        if corporate or boxes:
            holder = _one(header, ':scope > .article-content__articleDetailTitle__Kjfdx',
                          'article_attribution_not_verified')
            box = _one(holder, ':scope > .article-content__reviewer__vvt-C',
                       'article_attribution_not_verified')
            tag = _one(box, ':scope > .article-content__authorTag__RVK1L',
                       'article_attribution_not_verified')
            hover = _one(box, ':scope > .article-content__hover__YR39T',
                         'article_attribution_not_verified')
            link = _one(hover, ':scope > a.article-content__name__l8-t8',
                        'article_attribution_not_verified')
            reviewer_profile = MITRADE_ORIGIN + '/zh/insights/profile/mitrade-team'
            if (locale != 'zh' or len(boxes) != 1 or holder.find_all(recursive=False) != [box]
                    or box.find_all(recursive=False) != [tag, hover]
                    or hover.find_all(recursive=False) != [link]
                    or tag.get_text(' ', strip=True) != '審核人'
                    or link.get_text(' ', strip=True) != 'Mitrade' or link.get('href') != reviewer_profile
                    or box.get_text(' ', strip=True) != '審核人 Mitrade'
                    or 'reviewedBy' in declaration):
                raise NewsError('article_attribution_not_verified')
            _visible_tree(holder)
            reviewers.append({'native_role': '審核人', 'native_credit': 'Mitrade',
                'profile_url': reviewer_profile, 'role': 'publisher_declared_company_reviewer_not_verified_person',
                'basis': 'visible_native_header; no_JSONLD_reviewedBy_declaration',
                'independent_person_or_fact_check_verified': False})
    clocks = {}
    for key in ('datePublished', 'dateModified'):
        value = declaration.get(key)
        try:
            if not isinstance(value, str) or not _CLOCK.fullmatch(value):
                raise ValueError('undeclared or malformed UTC clock')
            clocks[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            raise NewsError('invalid_bulk_publication_clock') from None
    if clocks['dateModified'] < clocks['datePublished']:
        raise NewsError('invalid_bulk_publication_clock')
    con = _one(scope, ':scope > #articleCon')
    parts = con.find_all(recursive=False)
    native = _one(con, _CONTENT)
    # Five real siblings: site CTA, prose, dynamic recommendations, source
    # reuse notice and financial-risk notice. Unknown siblings are not ignored.
    if (len(parts) != 5 or any(n.name != 'div' for n in parts) or parts[1] is not native
            or any(str(n).strip() for n in con.find_all(string=True, recursive=False))
            or parts[4].get('class') != ['article-content__affirms__QFojR']
            or not re.fullmatch(r'[1-9][0-9]*', str(native.get('id', '')))):
        raise NewsError('article_identity_not_verified')
    context = _one(native, ':scope > .article-content__contextC__QMEJJ')
    if (native.find_all(recursive=False) != [context]
            or any(str(n).strip() for n in native.find_all(string=True, recursive=False))):
        raise NewsError('article_identity_not_verified')
    _visible_tree(context, table_columns=adapter_contract_version >= 3)
    for paragraph in context.find_all(['p', 'blockquote']):
        if re.match(r'^[（(]?(?:轉載自|转载自|轉自|本文轉載|本文转载|本文由|新聞來源[：:]|新闻来源[：:])',
                    paragraph.get_text(' ', strip=True)):
            raise NewsError('article_rights_or_scope_excluded')
    cta = _one(parts[0], ':scope > a')
    if (parts[0].find_all(recursive=False) != [cta]
            or cta.get('href') != MITRADE_ORIGIN + f'/{locale}/insights'
            or cta.get_text(' ', strip=True) != ('Mitrade.com 投资慧眼' if locale == 'cn' else 'Mitrade.com 投資慧眼')
            or [n.name for n in parts[2].find_all(recursive=False)] != ['p', 'div']
            or parts[2].find('p', recursive=False).get_text(' ', strip=True) != ('阅读更多' if locale == 'cn' else '閱讀更多')
            or parts[2].find('div', recursive=False).get('class') != ['article-content__readBox__DRJfg']):
        raise NewsError('article_identity_not_verified')
    _visible_tree(parts[0])
    _visible_tree(parts[3])
    _visible_tree(parts[4])
    reuse_links = parts[3].find_all('a', recursive=False)
    if (len(reuse_links) != 3 or reuse_links[0].get('href') != ''
            or reuse_links[0].get_text(' ', strip=True) != '《' + title + '》'
            or reuse_links[1].get('href') != MITRADE_ORIGIN + f'/{locale}/insights'
            or reuse_links[2].get('href') != MITRADE_ORIGIN + '/en'
            or not parts[4].get_text(' ', strip=True)):
        raise NewsError('article_identity_not_verified')
    # Two witnessed risk notices contain literal NULs: lxml drops them while
    # html.parser exposes them. They are non-visible controls, not prose or
    # numeric zeros. Normalize only notice text, retain the unchanged raw
    # response, and declare the projection instead of claiming byte equality.
    notices = [{'kind': kind, 'text': node.get_text(' ', strip=True).replace('\x00', ''),
                'text_projection_basis': 'native_visible_notice; NUL_controls_omitted; raw_response_unchanged',
                'open_license_or_bulk_permission_inferred': False}
               for kind, node in [('publisher_reuse_notice', parts[3]), ('publisher_financial_risk_notice', parts[4])]]
    ai_notice = False
    if adapter_contract_version >= 7:
        risk_parts = parts[4].find_all(recursive=False)
        ai_notice = bool(risk_parts and risk_parts[0].get_text(' ', strip=True) == _AI_ASSISTED_NOTICE)
        if ai_notice:
            if (len(risk_parts) != 2 or any(node.name != 'p' for node in risk_parts)
                    or any(str(n).strip() for n in parts[4].find_all(string=True, recursive=False))
                    or not risk_parts[1].get_text(' ', strip=True)):
                raise NewsError('article_identity_not_verified')
            notices[1]['text'] = risk_parts[1].get_text(' ', strip=True).replace('\x00', '')
            notices.append({'kind': 'publisher_AI_assisted_editorial_notice', 'text': _AI_ASSISTED_NOTICE,
                'basis': 'same_response_native_notice; publisher_declared_AI_assistance_and_editorial_proofreading',
                'human_author_or_fact_accuracy_independently_verified': False,
                'open_license_or_bulk_permission_inferred': False})
        if corporate and not ai_notice:
            # Only the witnessed company + reviewer + AI-notice combination is
            # admitted here. Missing notices cannot silently become human copy.
            raise NewsError('article_identity_not_verified')
    notices.append({'kind': 'publisher_current_machine_clock', 'visible_article_clock_present': False,
        'publication_raw': declaration['datePublished'], 'revision_raw': declaration['dateModified'],
        'precision': 'seconds', 'timezone': 'UTC', 'not_observed_historical_PIT': True})
    for image in list(context.select('img')):
        notices.append({'kind': 'unfetched_inline_image', 'media_requested': False,
                        'image_text_or_ownership_inferred': False})
        image.decompose()
    normalized = soup.new_tag('div', attrs={'class': 'mitrade-native-body'})
    while context.contents:
        normalized.append(context.contents[0].extract())
    body_scope = soup.new_tag('div')
    body_scope.append(normalized)
    return {
        'headline': title, 'author': [{'name': author['name']}],
        'datePublished': declaration['datePublished'], 'dateModified': declaration['dateModified'],
        'author_attribution_detail_json': json.dumps({'native_credit': signature, 'profile_url': profile,
            'publisher_schema_type': 'Person',
            'role': ('publisher_declared_company_author_not_verified_person' if corporate else
                     'publisher_declared_analysis_team_not_verified_person' if (locale, author['name']) in _V2_PROFILES
                     else 'publisher_declared_profile_not_verified_person'),
            'real_identity_independently_verified': False,
            **({'reviewers': reviewers, 'AI_assistance_declared': True if ai_notice else None}
               if reviewers or ai_notice else {})}, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': 'publisher_current_NewsArticle_UTC_seconds; '
            'visible_header_no_timestamp; not_observed_historical_PIT; no_URL_date_inference',
        'article_identity_basis': 'literal_locale_and_path_same_canonical_OG_schema_URL_'
            f'mainEntity_heading_head_title_reviewed_native_profile_and_ordered_body_v{adapter_contract_version}',
        'body_completeness': 'current_native_contextC_written_prose_tables_once; '
            'reuse_and_risk_notices_retained_separately; media_unfetched; '
            'dynamic_recommendations_CTA_cover_navigation_excluded; not_original_historical_version',
    }, body_scope
