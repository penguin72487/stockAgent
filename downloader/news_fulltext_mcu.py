"""Four reviewed MCU Weekly originals through the shared collector only."""
from __future__ import annotations

from datetime import datetime
import json
import re

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


MCU_IDS = ('43881', '45536', '45914', '45922')
MCU_PATH_PATTERN = '^/(?:43881|45536|45914|45922)/$'
MCU_BODY_SELECTOR = ':scope > .cm-post-content > .cm-entry-summary'
MCU_EDITOR = '編輯｜陳瑞斌'
MCU_PROFILE = 'https://www.week.mcu.edu.tw/author/jpchen/'
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00')


def mcu_visible_news(soup, *, page_url):
    """Keep native editor/desk roles and current revision, not a guessed writer.

    HTML declares source seconds/offset but the visible header is date-only.
    Neither declaration proves that this revised body existed on that date.
    Exact locally known URLs prevent expanding this review to campus reprints.
    """
    identities = {f'https://www.week.mcu.edu.tw/{identifier}/': identifier for identifier in MCU_IDS}
    identifier = identities.get(page_url)
    if identifier is None:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, f'article#post-{identifier}')
    content = _one(scope, ':scope > .cm-post-content.cm-single-post-content')
    heading = _one(content, ':scope > header.cm-entry-header > h1.cm-entry-title')
    title = heading.get_text(' ', strip=True)
    if (not title or _one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url
            or _one(soup, 'head > meta[property="og:title"]').get('content') != title
            or _one(soup, 'head > title').get_text(' ', strip=True)
                != title + ' – Ming Chuan Weekly 銘傳一週'):
        raise NewsError('article_identity_not_verified')
    panel = _one(content, ':scope > .cm-below-entry-meta')
    editor = _one(panel, ':scope > span.cm-author > a', 'article_attribution_not_verified')
    if editor.get('href') != MCU_PROFILE or editor.get_text(' ', strip=True) != MCU_EDITOR:
        raise NewsError('article_attribution_not_verified')
    posted = _one(panel, ':scope > .cm-post-date > a[rel=bookmark]')
    if posted.get('href') != page_url:
        raise NewsError('article_identity_not_verified')
    visible_clock = _one(posted, ':scope > time.entry-date')
    published = _one(soup, 'head > meta[property="article:published_time"]').get('content', '')
    modified = _one(soup, 'head > meta[property="article:modified_time"]').get('content', '')
    try:
        if (not _CLOCK.fullmatch(published) or not _CLOCK.fullmatch(modified)
                or datetime.fromisoformat(modified) < datetime.fromisoformat(published)
                or visible_clock.get('datetime') != published
                or visible_clock.get_text(' ', strip=True) != published[:10]
                or _one(scope, ':scope > meta[itemprop=dateModified]').get('content') != modified):
            raise ValueError('native clock declarations disagree')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    body = _one(scope, MCU_BODY_SELECTOR)
    for node in (scope, content, heading, panel, editor, posted, visible_clock, body):
        _visible(node)
    for figure in body.select('figure.wp-caption'):
        # These are separate image/caption units, not paragraphs of the report.
        # A changed container with prose must not be silently discarded.
        if (figure.parent is not body or figure.find('p') is not None
                or len(figure.select(':scope > img')) != 1
                or len(figure.select(':scope > figcaption')) != 1):
            raise NewsError('article_identity_not_verified')
        figure.decompose()
    for node in body.find_all(True):
        _visible(node)
        if node.name not in {'p', 'span', 'strong', 'br'}:
            raise NewsError('article_identity_not_verified')
    first = _one(body, ':scope > p:first-child').get_text(' ', strip=True)
    desk = '本刊訊' if identifier != '45536' else None
    if desk and not first.startswith('【本刊訊】'):
        raise NewsError('article_rights_or_scope_excluded')
    if not desk and re.match(r'^(?:轉載|轉自|本文來源|新聞來源|來源[：:])', first):
        raise NewsError('article_rights_or_scope_excluded')
    credits = ([desk] if desk else []) + [MCU_EDITOR]
    return {
        'headline': title, 'author': [{'name': credit} for credit in credits],
        'author_attribution_detail_json': json.dumps({
            'publication_desk_credit': desk, 'editor_credit': MCU_EDITOR,
            'editor_profile_url': MCU_PROFILE, 'named_reporter': None,
            'editor_is_not_inferred_reporter': True,
        }, ensure_ascii=False),
        'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'publisher_declared_machine_seconds_and_offset; visible_date_only; historical_release_observation_not_verified',
        'article_identity_basis': 'exact_known_MCU_URL_post_ID_canonical_OG_heading_and_editor_profile_v1',
        'body_completeness': 'reviewed_current_native_prose; separate_featured_photo_and_inline_photo_captions_excluded; not_original_historical_version',
    }, scope
