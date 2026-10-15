"""Small reviewed independent-news layouts, not separate clients or crawlers."""
from copy import deepcopy
from datetime import datetime
import json
import re

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


EVENTS_REVIEWED_URL = 'https://eventsinfocus.org/news/7145971'
EVENTS_BODY_SELECTOR = ':scope > .field--name-body, :scope > .field--name-field-footnote'


def eventsinfocus_visible_news(soup, *, page_url):
    """The one locally known own-reporter story, including its separate notes.

    Drupal's page also embeds fundraising bodies in the sidebar. Never use a
    page-wide .field--name-body selector, a donation/contact list, the abstract
    or an unrelated embedded node as the article. The visible release day and
    source machine datetime remain separate observations, not proven PIT.
    """
    if page_url != EVENTS_REVIEWED_URL:
        raise NewsError('article_identity_not_verified')
    canonical = _one(soup, 'link[rel~="canonical"]')
    ogurl = _one(soup, 'meta[property="og:url"][content]')
    if canonical.get('href') != page_url or ogurl.get('content') != page_url:
        raise NewsError('article_identity_not_verified')
    main = _one(soup, 'main#content[role="main"]')
    heading = _one(main, '#block-events2020-page-title h1.title > span.field--name-title')
    title = heading.get_text(' ', strip=True)
    ogtitle = _one(soup, 'meta[property="og:title"][content]')['content']
    if not title or re.sub(r'\s+', '', title) != re.sub(r'\s+', '', ogtitle):
        raise NewsError('article_identity_not_verified')
    article = _one(main, '#block-events2020-content > .content > article.node--type-news.node--view-mode-full')
    if article.get('about') != '/news/7145971' or article.get('data-history-node-id') != '7145971':
        raise NewsError('article_identity_not_verified')
    # This observed native page has no competing Article schema. Never let a
    # newly injected schema bypass its visible byline, date or complete prose.
    if soup.select('script[type="application/ld+json"]'):
        raise NewsError('article_identity_not_verified')
    content = _one(article, ':scope > .node__content')
    body = _one(content, ':scope > .field--name-body')
    notes = _one(content, ':scope > .field--name-field-footnote')
    clock = _one(content, ':scope > .field--name-field-time > time.datetime')
    day, machine_clock = clock.get_text(' ', strip=True), clock.get('datetime')
    try:
        if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', day):
            raise ValueError('unknown visible precision')
        visible_day = datetime.strptime(day, '%Y-%m-%d').date()
        if not isinstance(machine_clock, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z', machine_clock):
            raise ValueError('unknown declared datetime')
        declared = datetime.strptime(machine_clock, '%Y-%m-%dT%H:%M:%SZ')
        if declared.date() != visible_day:
            raise ValueError('competing source dates')
    except (ValueError, TypeError):
        raise NewsError('invalid_bulk_publication_clock') from None
    paragraphs = body.find_all('p', recursive=False)
    if not paragraphs or paragraphs[0] is not body.find(True):
        raise NewsError('article_attribution_not_verified')
    byline = paragraphs[0]
    credit = byline.get_text(' ', strip=True)
    if credit != '焦點事件記者王子豪報導':
        raise NewsError('article_rights_or_scope_excluded')
    if body.select('article, script, iframe, form, [rel~="license"]') or notes.select('article, script, iframe, form'):
        raise NewsError('article_rights_or_scope_excluded')
    for root in (main, heading, content, body, notes, clock):
        _visible(root)
    for root in (heading, body, notes, clock):
        for node in root.find_all(True):
            _visible(node)
    # Exactly one body copy; the source tree and raw receipt are unchanged.
    text_scope = deepcopy(content)
    text_scope.select_one(':scope > .field--name-body > p').decompose()
    return {
        'headline': title, 'author': [{'name': '王子豪'}], 'datePublished': day,
        'author_attribution_detail_json': json.dumps({'native_byline': credit,
            'role': '焦點事件記者', 'author_name_invented': False}, ensure_ascii=False),
        'published_time_basis': 'publisher_visible_date_only; source_machine_datetime_retained_separately; no_exact_historical_release_time_inferred',
        'verified_article_notices_json': json.dumps([{
            'kind': 'source_declared_machine_publication_datetime', 'text': machine_clock,
            'visible_date_raw': day, 'exact_historical_release_time_verified': False,
        }], ensure_ascii=False),
        'article_identity_basis': 'exact_known_URL_canonical_og_native_Drupal_ID_about_heading_and_reporter_v1',
        'body_completeness': 'verified_current_native_report_and_separate_footnotes; fundraising_sidebars_excluded',
    }, text_scope
