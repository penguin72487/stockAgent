"""Finite declared originals, not CMNews aliases or a second HTTP owner."""
from __future__ import annotations

from datetime import datetime
import json
import re

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


CMONEY_LEARNING_ORIGIN = 'https://cmoneylearning.com.tw'
# Preserve the publisher's literal percent spelling; do not widen URL identity.
CMONEY_LEARNING_PATHS = (
    '/%e6%9c%88%e9%85%8d%e6%81%afetf%e7%bc%ba%e9%bb%9e%e6%9c%89%e5%93%aa%e4%ba%9b%ef%bc%9f%e9%99%a4%e6%81%af%e6%89%a3%e6%b7%a8%e5%80%bc%e3%80%81%e4%ba%8c%e4%bb%a3%e5%81%a5%e4%bf%9d%e9%96%80%e6%aa%bb/',
    '/%e6%88%bf%e8%b2%b8%e8%bd%89%e8%b2%b8%e6%88%90%e6%9c%ac%e6%9c%89%e5%93%aa%e4%ba%9b%ef%bc%9f5%e5%a4%a7%e9%9a%b1%e5%bd%a2%e8%b2%bb%e7%94%a8%e8%88%87%e9%81%95%e7%b4%84%e9%87%91%e8%a9%a6%e7%ae%97%ef%bc%9a/',
)
CMONEY_LEARNING_PATH_PATTERN = '^(?:' + '|'.join(re.escape(p) for p in CMONEY_LEARNING_PATHS) + ')$'
# These two literal declarations already have private metadata receipts. They
# are not guessed slugs, alias equivalence, or permission for site discovery.
CMONEY_LEARNING_SUPPLEMENT_PATHS = (
    '/00907%e9%85%8d%e6%81%af0-24%e5%85%83%ef%bc%8c8%e6%9c%8825%e6%97%a5%e9%99%a4%e6%81%af%e5%89%8d%e8%a9%b2%e8%b2%b7%e5%97%8e%ef%bc%9f/',
    '/00888%e9%85%8d%e6%81%af%e6%80%8e%e9%ba%bc%e9%a0%98%ef%bc%9f%e4%b8%80%e5%bc%b5%e8%83%bd%e9%a0%98%e5%a4%9a%e5%b0%91%e3%80%81%e6%9c%80%e5%be%8c%e8%b2%b7%e9%80%b2%e6%97%a5%e6%80%8e%e9%ba%bc%e7%9c%8b/',
)
CMONEY_LEARNING_V2_PATHS = CMONEY_LEARNING_PATHS + CMONEY_LEARNING_SUPPLEMENT_PATHS
CMONEY_LEARNING_V2_PATH_PATTERN = '^(?:' + '|'.join(re.escape(p) for p in CMONEY_LEARNING_V2_PATHS) + ')$'
CMONEY_LEARNING_V2_AUTH_SCOPE = (
    'Bounded private personal research of the initial two public source-declared originals and exactly '
    'two additional literal originals declared in retained CMNews metadata. New originals require '
    'explicit source/body/credit/clock verification and shared immutable supplemental receipts. '
    'No paid/member, third-party rights override, publication, training export, alias equivalence or whole-site scope. '
    'Not a reuse license or fair-use verdict; preserve credits, REP and actual refusals.'
)
CMONEY_LEARNING_V2_NOTE = (
    'The original two-URL finite cohort and completed bodies remain unchanged. Only two additional literal '
    'CMNews-declared originals can enter reviewed supplements; no site discovery, guessed slugs, AI polling, '
    'public body, automatic training export or historical-release guarantee. An external canonical is discovery, '
    'not CMNews alias equivalence. Ten-second local spacing is not an official or measured rate maximum.'
)
# Exact new capture/auth -> prior HTTP measurement identity. Populated only
# after the complete closed candidate configuration has been independently
# reconstructed; any other scope, account, endpoint or pacing edit cannot
# borrow the old refusal state through this binding.
CMONEY_LEARNING_V2_HTTP_BINDING = (
    'd11e6ac4f2615fe1a2438c76fe62cf796f3d8c7b9f858b404c88efb15718a2f8',
    '143a6bc35e1895c1287185d9b299807d50832f08b2716bbb9c9cfdabe4d498bc',
    'bf0cc28c179eaed24d24ded96ce76279516fcf07e880a580c94e721e5967fc57',
    '00e51936c94fc5ed1c7d7a06eb2bac3455ce4cfaaf4240a259fe45f6881bc258',
)
# Actual article hrefs in the four already retained originals. These are a
# closed review cohort, not slug guesses, CMNews aliases or site discovery.
# The initial two-member cohort and the completed two-member supplement stay
# immutable; this version only permits a separately verified supplement.
CMONEY_LEARNING_NAVIGATION_PATHS = (
    '/%e6%b3%95%e5%9c%8b%e6%9a%b4%e5%8b%95%e5%8e%9f%e5%9b%a0%e6%98%af%e4%bb%80%e9%ba%bc%ef%bc%9f%e7%94%9f%e6%b4%bb%e6%88%90%e6%9c%ac%e3%80%81%e6%95%99%e8%82%b2%e4%b8%8d%e5%b9%b3%e7%ad%89%e8%88%87%e8%b3%87/',
    '/%e4%bf%a1%e7%94%a8%e5%8d%a1%e9%a1%8d%e5%ba%a6%e8%aa%bf%e6%95%b4%e8%a6%81%e6%ba%96%e5%82%99%e4%bb%80%e9%ba%bc%e8%b3%87%e6%96%99%ef%bc%9f%e8%b2%a1%e5%8a%9b%e8%ad%89%e6%98%8e%e3%80%81%e7%94%b3%e8%ab%8b/',
    '/0056%e9%85%8d%e6%81%af%e6%80%8e%e9%ba%bc%e7%9c%8b%ef%bc%9f%e4%b8%80%e5%bc%b5%e8%83%bd%e9%a0%98%e5%a4%9a%e5%b0%91%e3%80%81%e9%99%a4%e6%81%af%e5%be%8c%e7%82%ba%e4%bd%95%e4%b8%8d%e7%ad%89%e6%96%bc/',
    '/%e5%be%85%e6%a5%ad%e6%9c%9f%e9%96%93%e6%8e%9b%e5%b7%a5%e6%9c%83%e6%8a%95%e4%bf%9d%e5%a5%bd%e5%97%8e%ef%bc%9f%e6%af%8f%e6%9c%88%e4%bf%9d%e8%b2%bb%e6%94%af%e5%87%ba%e3%80%81%e5%8b%9e%e4%bf%9d%e9%80%80/',
    '/00990b%e9%85%8d%e6%81%af%e6%80%8e%e9%ba%bc%e7%9c%8b%ef%bc%9f%e4%b8%80%e5%bc%b5%e9%a0%90%e4%bc%b080%e5%85%83%ef%bc%8c%e6%9c%88%e9%85%8d%e6%81%af%e8%83%bd%e7%95%b6%e5%9b%ba%e5%ae%9a%e6%94%b6%e5%85%a5/',
    '/00726b%e9%85%8d%e6%81%af%e6%80%8e%e9%ba%bc%e9%a0%98%ef%bc%9f%e4%b8%80%e5%bc%b5%e9%a0%90%e4%bc%b0460%e5%85%83%ef%bc%8c%e6%8a%95%e7%ad%89%e5%82%b5%e6%9c%89%e5%93%aa%e4%ba%9b%e9%a2%a8%e9%9a%aa/',
    '/%e8%87%aa%e5%be%8b%e7%a5%9e%e7%b6%93%e5%a4%b1%e8%aa%bf%e6%9c%83%e5%a5%bd%e5%97%8e%ef%bc%9f%e6%aa%a2%e6%9f%a5%e5%85%a8%e6%ad%a3%e5%b8%b8%e5%8d%bb%e5%a4%a9%e5%a4%a9%e5%bf%83%e6%82%b8%e9%a0%ad%e6%9a%88/',
    '/%e9%95%b7%e8%80%85%e9%98%b2%e8%b7%8c%e8%82%8c%e5%8a%9b%e9%81%8b%e5%8b%95%e6%95%99%e5%ad%b8%ef%bc%9a%e5%a4%a7%e8%85%bf%e7%84%a1%e5%8a%9b%e8%b5%b0%e8%b7%af%e7%a2%8e%e6%ad%a5%e5%bf%83%e6%85%8c%ef%bc%9f/',
    '/%e8%81%b7%e5%a0%b4%e5%86%b7%e6%9a%b4%e5%8a%9b%e9%80%bc%e9%80%80%e4%b8%bb%e7%ae%a1%e6%95%85%e6%84%8f%e4%b8%8d%e6%8c%87%e6%b4%be%e5%b7%a5%e4%bd%9c%ef%bc%9f%e5%bf%83%e7%90%86%e5%b8%ab%e6%8f%ad%e6%9a%97/',
    '/%e5%ad%a9%e5%ad%90%e6%b5%81%e6%84%9f%e8%a6%81%e8%ab%8b%e5%81%87%e5%97%8e%ef%bc%9f%e5%85%88%e7%9c%8b3%e5%80%8b%e8%bf%94%e6%a0%a1%e6%a2%9d%e4%bb%b6%ef%bc%8c%e6%8e%9b%e6%80%a5%e8%a8%ba%e5%be%8c%e9%9a%94/',
    '/meta%e6%99%ba%e6%85%a7%e7%9c%bc%e9%8f%a1%e5%af%a6%e6%b8%ac%ef%bc%9a%e9%9a%b1%e8%97%8f%e8%9e%a2%e5%b9%95%e6%8a%ac%e9%a0%ad%e9%a1%af%e7%a4%ba%ef%bc%8c%e5%85%89%e5%ad%b8%e5%be%ae%e6%8a%95%e5%bd%b1/',
    '/2026%e4%b9%9d%e5%90%88%e4%b8%80%e9%81%b8%e8%88%89%e6%8a%95%e7%a5%a811-28%e5%85%ad%e5%b8%b6%e4%bb%80%e9%ba%bc%ef%bc%9f%e5%bf%85%e5%82%993%e6%a8%a3%e7%89%a9%e5%93%81%e8%88%87%e6%8a%95%e7%a5%a8/',
    '/2027%e5%b9%b4%e8%8f%9c%e9%a0%90%e8%b3%bc%e5%93%aa%e8%a3%a1%e8%b2%b7%ef%bc%9f4%e5%a4%a7%e9%80%9a%e8%b7%af%e6%94%b6%e5%96%ae%e6%99%82%e7%a8%8b%e3%80%81%e4%bd%9b%e8%b7%b3%e7%89%86%e3%80%81%e7%b1%b3/',
    '/%e7%b5%90%e5%a9%9a%e5%85%8d%e8%81%98%e9%87%91%e5%8f%aa%e8%a6%81%e7%a7%9f%e6%88%bf%e7%8d%a8%e7%ab%8b%ef%bc%9f%e5%a9%86%e5%ae%b6%e6%80%92%e6%96%a5%e6%8a%8a%e4%ba%ba%e6%95%b4%e7%a2%97%e7%ab%af%e8%b5%b0/',
    '/%e5%9b%9b%e9%80%b1%e5%bd%88%e6%80%a7%e5%b7%a5%e6%99%82%e8%aa%bf%e7%a7%bb%e9%9b%87%e4%b8%bb%e8%83%bd%e7%89%87%e9%9d%a2%e6%8c%87%e5%ae%9a%e5%97%8e%ef%bc%9f%e6%8e%92%e7%8f%ad%e8%a6%8f%e5%89%87%e3%80%81/',
)
CMONEY_LEARNING_V3_PATHS = CMONEY_LEARNING_V2_PATHS + CMONEY_LEARNING_NAVIGATION_PATHS
CMONEY_LEARNING_V3_PATH_PATTERN = '^(?:' + '|'.join(re.escape(p) for p in CMONEY_LEARNING_V3_PATHS) + ')$'
CMONEY_LEARNING_V3_AUTH_SCOPE = (
    'Bounded private personal research of the four completed source-declared originals and exactly '
    'fifteen literal article hrefs witnessed in those retained originals. Each new original requires '
    'explicit same-response source/body/credit/clock verification and shared immutable supplemental receipts. '
    'No paid/member, third-party rights override, publication, training export, alias equivalence or whole-site scope. '
    'Not a reuse license or fair-use verdict; preserve credits, REP and actual refusals.'
)
CMONEY_LEARNING_V3_NOTE = (
    'Reuse all four completed originals and the frozen original cohort/supplement. Only fifteen already '
    'retained literal article hrefs may enter separately reviewed supplements, finance/retirement first. '
    'No recursive discovery, guessed slugs, AI polling, public body, automatic training export, CMNews alias '
    'equivalence or historical-release guarantee. Ten-second local spacing is not a measured source maximum.'
)
# Exact reconstructed v3 capture/auth -> unchanged v1 transport/refusals.
CMONEY_LEARNING_V3_HTTP_BINDING = (
    'b20468af11709f94fe029cad2d3a5e5891e1e64b266fea549f279efb01d15d38',
    '6e0743155a051f16805f51fb9461b0b25d1b05c504b06cb12912cfafd9cf8703',
    'bf0cc28c179eaed24d24ded96ce76279516fcf07e880a580c94e721e5967fc57',
    '00e51936c94fc5ed1c7d7a06eb2bac3455ce4cfaaf4240a259fe45f6881bc258',
)
# One new literal canonical declaration from the completed, fixed CMNews
# relation witness. Old nineteen paths and their finite receipts are unchanged.
CMONEY_LEARNING_DECLARED_20261023_PATH = '/00958b%e9%85%8d%e6%81%af%e6%80%8e%e9%ba%bc%e7%9c%8b%ef%bc%9f%e4%b8%80%e5%bc%b5%e8%83%bd%e9%a0%98%e5%a4%9a%e5%b0%91%e3%80%81%e6%9c%88%e9%85%8d%e9%8a%80%e8%a1%8c%e5%82%b5%e6%9c%89%e5%93%aa%e4%ba%9b/'
CMONEY_LEARNING_V4_PATHS = CMONEY_LEARNING_V3_PATHS + (CMONEY_LEARNING_DECLARED_20261023_PATH,)
CMONEY_LEARNING_V4_PATH_PATTERN = '^(?:' + '|'.join(re.escape(p) for p in CMONEY_LEARNING_V4_PATHS) + ')$'
CMONEY_LEARNING_V4_AUTH_SCOPE = (
    'Bounded private personal research of the nineteen completed originals and exactly one additional '
    'literal canonical original declared by the completed CMNews 00958B relation witness. '
    'The new original requires explicit same-response source/body/credit/clock verification and a shared '
    'immutable supplemental receipt. No paid/member, third-party rights override, publication, training '
    'export, alias equivalence or whole-site scope. Not a reuse license or fair-use verdict; '
    'preserve credits, REP and actual refusals.'
)
CMONEY_LEARNING_V4_NOTE = (
    'Reuse the nineteen completed originals, frozen cohort and supplements. Only the new literal '
    'CMNews-declared 00958B original may enter a separately verified supplement. No recursive '
    'discovery, guessed slugs, AI polling, public body, automatic training export, CMNews alias '
    'equivalence or historical-release guarantee. Ten-second local spacing is not a measured source maximum.'
)
CMONEY_LEARNING_V4_HTTP_BINDING = (
    'a2509b909c5b0fe32e0bc6cc13a7e5843b2da2037fc5ffa43cf5b34a9299b94f',
    'ea39c3f35f3e22a2b4582c8c37ab8f5a23ba1bb3745025dc325d070ca16bb4b9',
    'bf0cc28c179eaed24d24ded96ce76279516fcf07e880a580c94e721e5967fc57',
    '00e51936c94fc5ed1c7d7a06eb2bac3455ce4cfaaf4240a259fe45f6881bc258',
)
CMONEY_LEARNING_BODY_SELECTOR = ':scope > .articleContent__body > .articleContent__content--main > .articleContent__textContent'
_EDITOR = '理財內容編輯林序安'
_PERSON = CMONEY_LEARNING_ORIGIN + '/author/rowan_lin/#person'
_ORGANIZATION = CMONEY_LEARNING_ORIGIN + '/#organization'
_CLOCK = r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}'


def _clock(value):
    try:
        if not isinstance(value, str) or not re.fullmatch(_CLOCK, value):
            raise ValueError('unreviewed source precision')
        return datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None


def cmoneylearning_visible_article(soup, *, page_url, adapter_contract_version=1):
    """Join native identity, visible byline/clock and its own BlogPosting.

    The visible machine clock has +08:00, whereas JSON-LD uses +00:00. Compare
    instants, not literal days, and preserve both actual declared values. The
    credit is a source declaration, not an inferred reporter or historical
    witness. V1/v2 keep their reviewed single-editor rule; the closed v3
    navigation cohort joins its own visible credit to exactly one Person.
    """
    paths = {1: CMONEY_LEARNING_PATHS, 2: CMONEY_LEARNING_V2_PATHS,
             3: CMONEY_LEARNING_V3_PATHS, 4: CMONEY_LEARNING_V4_PATHS}.get(adapter_contract_version, ())
    if (type(adapter_contract_version) is not int
            or page_url not in {CMONEY_LEARNING_ORIGIN + p for p in paths}):
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup, 'head > link[rel=canonical]')
    scope = _one(soup, 'main.site-main article.articleContent')
    main = _one(scope, ':scope > .articleContent__body > .articleContent__content--main')
    heading = _one(main, ':scope > .articleContent__content--titleRow > h1.articleContent__title')
    title = heading.get_text(' ', strip=True)
    head_title = _one(soup, 'head > title').get_text(' ', strip=True)
    og_title = _one(soup, 'head > meta[property="og:title"]').get('content')
    suffix = ' | 實習大人'
    if (canonical.get('href') != page_url or not title
            or head_title != og_title or not head_title.endswith(suffix)
            or len(head_title) <= len(suffix)
            or adapter_contract_version == 1 and head_title != title + suffix
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'):
        raise NewsError('article_identity_not_verified')
    body = _one(scope, CMONEY_LEARNING_BODY_SELECTOR)
    meta = _one(main, ':scope > .articleContent__meta > .articleContent__metaLine')
    byline = _one(meta, ':scope > .articleContent__author', 'article_attribution_not_verified')
    credit = byline.get_text(' ', strip=True)
    if (not credit or len(credit) > 120 or re.search(r'[\x00-\x1f\x7f]', credit)
            or adapter_contract_version < 3 and credit != _EDITOR):
        raise NewsError('article_attribution_not_verified')
    date = _one(meta, ':scope > time.articleContent__date', 'invalid_bulk_publication_clock')
    published = _clock(date.get('datetime'))
    human_dates = {published.strftime('%Y年%m月%d日')}
    if adapter_contract_version >= 3:
        # Native human dates omit padding; the actual machine timestamp and
        # its same-response BlogPosting instant remain exact and unchanged.
        human_dates.add(f'{published.year}年{published.month}月{published.day}日')
    if (published.strftime('%z') != '+0800'
            or date.get_text(' ', strip=True) not in human_dates):
        raise NewsError('invalid_bulk_publication_clock')

    declarations = []
    try:
        for script in soup.select('script[type="application/ld+json"]'):
            declarations.extend(_objects(json.loads(script.get_text())))
    except (ValueError, TypeError):
        raise NewsError('article_identity_not_verified') from None

    def declared(kind):
        matches = [v for v in declarations if v.get('@type') == kind]
        if len(matches) != 1:
            raise NewsError('article_identity_not_verified')
        return matches[0]

    article, person, organization = declared('BlogPosting'), declared('Person'), declared('Organization')
    person_id = _PERSON
    if adapter_contract_version >= 3:
        person_id = person.get('@id')
        if (not isinstance(person_id, str) or not re.fullmatch(
                re.escape(CMONEY_LEARNING_ORIGIN) + r'/author/[a-z0-9_-]{1,64}/#person', person_id)
                or person.get('name') != credit
                or person.get('url') != person_id.removesuffix('#person')
                or article.get('author') != {'@id': person_id}):
            raise NewsError('article_attribution_not_verified')
    if (article.get('@id') != page_url + '#article'
            or article.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or article.get('headline') != (title if adapter_contract_version == 1 else head_title[:-len(suffix)])):
        raise NewsError('article_identity_not_verified')
    title_variants = head_title != title + suffix
    if title_variants:
        # Current visible and SEO/BlogPosting headings may differ. Require an
        # independent same-response breadcrumb witness, not a fuzzy title join.
        # Their historical chronology remains unknown and is never inferred.
        crumbs = declared('BreadcrumbList').get('itemListElement')
        if (not isinstance(crumbs, list) or not 2 <= len(crumbs) <= 16
                or any(not isinstance(item, dict) or item.get('@type') != 'ListItem'
                       or type(item.get('position')) is not int or item['position'] != index
                       for index, item in enumerate(crumbs, 1))
                or crumbs[0].get('item') != CMONEY_LEARNING_ORIGIN + '/'
                or crumbs[-1].get('name') != title or crumbs[-1].get('item') not in (None, page_url)):
            raise NewsError('article_identity_not_verified')
    if article.get('isAccessibleForFree') is not True:
        raise NewsError('paywall_or_login')
    if (article.get('author') != {'@id': person_id} or person.get('@id') != person_id
            or person.get('name') != credit or person.get('url') != person_id.removesuffix('#person')
            or person.get('worksFor') != {'@id': _ORGANIZATION}
            or article.get('publisher') != {'@id': _ORGANIZATION}
            or article.get('copyrightHolder') != {'@id': _ORGANIZATION}
            or organization.get('@id') != _ORGANIZATION or organization.get('name') != '實習大人'
            or organization.get('url') != CMONEY_LEARNING_ORIGIN + '/'):
        raise NewsError('article_rights_or_scope_excluded')
    if _clock(article.get('datePublished')) != published:
        raise NewsError('invalid_bulk_publication_clock')
    if _clock(article.get('dateModified')) < published:
        raise NewsError('invalid_bulk_publication_clock')
    decorative = set()
    if adapter_contract_version >= 3:
        # Actual native YouTube facade play icons contain two empty vector
        # paths, not hidden prose. Do not load a player, execute JS, infer a
        # transcript, or exempt arbitrary hidden SVG/HTML descendants.
        for svg in body.select('span.yt-facade__play > svg[aria-hidden="true"][focusable="false"]'):
            paths = svg.find_all(True)
            if (set(svg.attrs) <= {'aria-hidden', 'focusable', 'viewbox'}
                    and svg.get('viewbox') == '0 0 68 48' and not svg.get_text(strip=True)
                    and len(paths) == 2 and all(path.name == 'path' and path.parent is svg
                        and set(path.attrs) <= {'d', 'class', 'fill'} and not path.find(True)
                        for path in paths)):
                _visible(svg.parent)
                decorative.update(id(node) for node in (svg, *paths))
    for node in (scope, main, heading, meta, byline, date, body, *body.find_all()):
        if id(node) in decorative:
            continue
        _visible(node)
        if any(set(n.get('class', [])) & {'hidden', 'invisible', 'sr-only'}
               for n in (node, *node.parents) if getattr(n, 'attrs', None)):
            raise NewsError('article_identity_not_verified')
    # The observed empty shortcode is a generated-content widget, not source
    # prose. Never execute its poller or call its endpoint to complete a body.
    for widget in body.select('article.readmo-ai-article-shortcode'):
        if widget.get_text(strip=True) or widget.find(True):
            raise NewsError('article_identity_not_verified')
    if body.select('vip-content, .utilsMask, iframe, [data-paywall]'):
        raise NewsError('paywall_or_login')
    return {
        'headline': title, 'author': [{'name': credit}],
        'datePublished': date['datetime'], 'dateModified': article['dateModified'],
        'author_attribution_detail_json': json.dumps({
            'visible_credit_raw': credit, 'source_person_id': person_id,
            'declared_editor_role_not_inferred_reporter': credit == _EDITOR and person_id == _PERSON,
            **({'same_response_visible_Person_credit_not_inferred_role': True}
               if person_id != _PERSON or credit != _EDITOR else {}),
        }, ensure_ascii=False, sort_keys=True),
        'published_time_basis': 'native_visible_machine_clock_seconds_and_offset_matched_to_BlogPosting_instant; no_historical_release_observation',
        'article_identity_basis': f'exact_reviewed_literal_canonical_heading_and_current_BlogPosting_v{adapter_contract_version}; not_CMNews_alias_equivalence',
        **({'source_title_declarations_json': json.dumps({
            'contract_version': 1, 'visible_title_raw': title, 'head_title_raw': head_title,
            'og_title_raw': og_title, 'BlogPosting_headline_raw': article['headline'],
            'title_variants_present': title_variants, 'selected_title_basis': 'visible_native_h1',
            'current_breadcrumb_heading_witness_required': title_variants,
            'historical_title_chronology_inferred': False,
        }, ensure_ascii=False, sort_keys=True)} if adapter_contract_version >= 2 else {}),
        'body_completeness': 'current_public_SSR_prose_and_tables; title_byline_hero_footer_and_inert_AI_widget_excluded',
    }, scope
