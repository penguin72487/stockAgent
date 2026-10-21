"""Finite, source-verified NCHU press-release text; never a second HTTP owner."""
from __future__ import annotations

from datetime import datetime
import json
import re

from bs4 import NavigableString

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


NCHU_REVIEWED_URL = 'https://www2.nchu.edu.tw/news-detail/id/55817'
NCHU_BODY_SELECTOR = ':scope > .article > .news-detail > .editor > .spec-skin'
NCHU_PATH_PATTERN = '^/news-detail/id/55817$'
_CLOCK = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'


def _label(scope, selector, expected):
    node = _one(scope, selector, 'article_attribution_not_verified')
    _visible(node)
    labels = node.find_all(recursive=False)
    if len(labels) != 1 or labels[0].name != 'span' or labels[0].get_text(strip=True) != expected:
        raise NewsError('article_attribution_not_verified')
    _visible(labels[0])
    value = ''.join(str(n) for n in node.contents if type(n) is NavigableString).strip()
    return value


def nchu_visible_news(soup, *, page_url):
    """Match one locally known original; reject media republication and drift.

    The CMS calls the two clocks 更新時間 and 張貼時間. Preserve both with their
    source seconds, without inventing UTC offsets or borrowing the event date.
    秘書室媒體公關組 is an explicitly credited organization, not a named reporter;
    興新聞張貼者 is only a posting-account label. Media and album links are not prose.
    """
    if page_url != NCHU_REVIEWED_URL:
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'main#content > #news > .page-wrap > .intro-wrap > .detail')
    heading = _one(scope, ':scope > h1.info-title')
    title = heading.get_text(' ', strip=True)
    if not title.startswith('【公關組】'):
        raise NewsError('article_rights_or_scope_excluded')
    og_url = _one(soup, 'head > meta[property="og:url"]')
    og_title = _one(soup, 'head > meta[property="og:title"]')
    page_title = _one(soup, 'head > title').get_text(' ', strip=True)
    if (og_url.get('content') != page_url or og_title.get('content') != title
            or page_title != title + ' - 國立中興大學(National Chung Hsing University)'):
        raise NewsError('article_identity_not_verified')
    canonicals = soup.select('head > link[rel=canonical]')
    if canonicals and (len(canonicals) != 1 or canonicals[0].get('href') != page_url):
        raise NewsError('article_identity_not_verified')
    panel = _one(scope, ':scope > .article > .article-head > .part1')
    if (_label(panel, ':scope > .unit', '單位') != '秘書室'
            or _label(panel, ':scope > .source', '新聞來源') != '秘書室媒體公關組'):
        raise NewsError('article_rights_or_scope_excluded')
    poster = _one(panel, ':scope > .postBy', 'article_attribution_not_verified')
    if poster.get_text(' ', strip=True) != '興新聞張貼者':
        raise NewsError('article_attribution_not_verified')
    date = _one(panel, ':scope > .date', 'invalid_bulk_publication_clock')
    if date.find(True):
        raise NewsError('invalid_bulk_publication_clock')
    clock = re.fullmatch(r'更新時間：(' + _CLOCK + r')\s*/\s*張貼時間：(' + _CLOCK + r')',
                         date.get_text(' ', strip=True))
    try:
        if clock is None:
            raise ValueError('unreviewed native clock labels or precision')
        modified, published = clock.groups()
        if datetime.strptime(modified, '%Y-%m-%d %H:%M:%S') < datetime.strptime(published, '%Y-%m-%d %H:%M:%S'):
            raise ValueError('revision precedes publication')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    body = _one(scope, NCHU_BODY_SELECTOR)
    gallery = _one(body, ':scope > .figure-group')
    album = _one(body, ':scope > strong > a[href="https://sec.video.nchu.edu.tw/media/1248"]')
    if album.get_text(' ', strip=True) != '活動相簿':
        raise NewsError('article_identity_not_verified')
    for node in (scope, heading, panel, poster, date, body):
        _visible(node)
    # Remove only source-observed non-prose siblings in this exact reviewed
    # release. Raw HTML remains immutable in the canonical response receipt.
    gallery.decompose()
    album.parent.decompose()
    for node in body.find_all(True):
        _visible(node)
        if node.name not in {'br', 'strong'}:
            raise NewsError('article_identity_not_verified')
    if not body.get_text(' ', strip=True):
        raise NewsError('body_not_found')
    return {
        'headline': title,
        'author': [{'name': '秘書室媒體公關組'}],
        'author_attribution_detail_json': json.dumps({
            'organization': '秘書室媒體公關組', 'release_unit': '秘書室',
            'posting_account_label': poster.get_text(' ', strip=True),
            'named_person_author': None,
        }, ensure_ascii=False),
        'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'publisher_declared_local_clock_seconds; UTC_offset_not_declared_or_inferred',
        'article_identity_basis': 'exact_reviewed_NCHU55817_og_url_matching_native_heading_title_and_own_press_office_v1',
        'body_completeness': 'verified_current_own_agency_prose; separate_photo_gallery_captions_and_album_control_excluded',
    }, scope
