"""Two reviewed original articles, not CMNews aliases or a second HTTP owner."""
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


def cmoneylearning_visible_article(soup, *, page_url):
    """Join native identity, visible byline/clock and its own BlogPosting.

    The visible machine clock has +08:00, whereas JSON-LD uses +00:00. Compare
    instants, not literal days, and preserve both actual declared values. The
    editor is a source credit, not a claimed reporter or historical witness.
    """
    if page_url not in {CMONEY_LEARNING_ORIGIN + p for p in CMONEY_LEARNING_PATHS}:
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup, 'head > link[rel=canonical]')
    scope = _one(soup, 'main.site-main article.articleContent')
    main = _one(scope, ':scope > .articleContent__body > .articleContent__content--main')
    heading = _one(main, ':scope > .articleContent__content--titleRow > h1.articleContent__title')
    title = heading.get_text(' ', strip=True)
    if (canonical.get('href') != page_url or not title
            or _one(soup, 'head > title').get_text(' ', strip=True) != title + ' | 實習大人'
            or _one(soup, 'head > meta[property="og:title"]').get('content') != title + ' | 實習大人'
            or _one(soup, 'head > meta[property="og:type"]').get('content') != 'article'):
        raise NewsError('article_identity_not_verified')
    body = _one(scope, CMONEY_LEARNING_BODY_SELECTOR)
    meta = _one(main, ':scope > .articleContent__meta > .articleContent__metaLine')
    byline = _one(meta, ':scope > .articleContent__author', 'article_attribution_not_verified')
    if byline.get_text(' ', strip=True) != _EDITOR:
        raise NewsError('article_attribution_not_verified')
    date = _one(meta, ':scope > time.articleContent__date', 'invalid_bulk_publication_clock')
    published = _clock(date.get('datetime'))
    if (published.strftime('%z') != '+0800'
            or date.get_text(' ', strip=True) != published.strftime('%Y年%m月%d日')):
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
    if (article.get('@id') != page_url + '#article'
            or article.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or article.get('headline') != title):
        raise NewsError('article_identity_not_verified')
    if article.get('isAccessibleForFree') is not True:
        raise NewsError('paywall_or_login')
    if (article.get('author') != {'@id': _PERSON} or person.get('@id') != _PERSON
            or person.get('name') != _EDITOR or person.get('url') != _PERSON.removesuffix('#person')
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
    for node in (scope, main, heading, meta, byline, date, body, *body.find_all()):
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
        'headline': title, 'author': [{'name': _EDITOR}],
        'datePublished': date['datetime'], 'dateModified': article['dateModified'],
        'author_attribution_detail_json': json.dumps({
            'visible_credit_raw': _EDITOR, 'source_person_id': _PERSON,
            'declared_editor_role_not_inferred_reporter': True,
        }, ensure_ascii=False, sort_keys=True),
        'published_time_basis': 'native_visible_machine_clock_seconds_and_offset_matched_to_BlogPosting_instant; no_historical_release_observation',
        'article_identity_basis': 'exact_reviewed_literal_canonical_heading_and_current_BlogPosting_v1; not_CMNews_alias_equivalence',
        'body_completeness': 'current_public_SSR_prose_and_tables; title_byline_hero_footer_and_inert_AI_widget_excluded',
    }, scope
