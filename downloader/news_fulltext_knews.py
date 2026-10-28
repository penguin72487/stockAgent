"""Knews public originals, with distinct header and printed credits.

V1 keeps the two reviewed originals; V2 admits literal known article URLs;
V3 also keeps native inline captions, quotations and editors without headings.
V4 keeps source-declared editorial desks, profile slugs, original clock order
and fully witnessed nested recommendation cards distinct from article prose.
V5 retains raw title-edge whitespace, printed reporter/translator roles and
complete short native editors without imposing an arbitrary paragraph length.
V6 retains complete written cards, both finance-desk separators and a verified
nontext video mount without fetching or transcribing its media.
V8 retains native table highlight text and a literally printed reporter
signature missing its opening bracket, without repairing the source spelling.
V9 retains whole native data grids, skips inert card comments and verifies
nontext HLS mounts without fetching a poster, stream or transcript.
This pure native parser
does not fetch anything, infer that different names identify the same person,
or turn a public response into a publisher-wide permission or historical clock.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import parse_qsl, urlsplit

from bs4 import Comment, Tag

from downloader.news_fulltext_policy import NewsError, _objects
from downloader.news_fulltext_public_originals import _one, _visible


KNEWS_LAYOUT = 'knews_public_native_article_v1'
KNEWS_KNOWN_URL_LAYOUT = 'knews_public_native_article_v2'
KNEWS_EDITOR_LAYOUT = 'knews_public_native_article_v3'
KNEWS_DECLARATIONS_LAYOUT = 'knews_public_native_article_v4'
KNEWS_CREDITS_LAYOUT = 'knews_public_native_article_v5'
KNEWS_WRITTEN_BLOCKS_LAYOUT = 'knews_public_native_article_v6'
KNEWS_SPARSE_NATIVE_LAYOUT = 'knews_public_native_article_v7'
KNEWS_TABLE_NATIVE_LAYOUT = 'knews_public_native_article_v8'
KNEWS_GRID_NATIVE_LAYOUT = 'knews_public_native_article_v9'
KNEWS_MULTI_REPORTER_LAYOUT = 'knews_public_native_article_v10'
KNEWS_V9_VERIFICATION = 'native_whole_written_grid_inert_card_comments_and_nontext_HLS_v9_20261026'
KNEWS_V10_VERIFICATION = 'native_ordered_multiple_reporters_literal_profiles_schema_and_meta_v10_20261026'
KNEWS_GRID_HTTP_BINDING = (
    'ab29972e3b5f7091ded056bf2eb5049bb1f9551ca575708bf3f272567f87767e',
    'a74543def531ea312d511d3fe462e30b17d64c2a087f2ea922bba3cf830c8de4',
    '4b64e6ca583ff8dadef050f6412bec1f773ed3166da81c489e52f2af154c3080',
    'f7f59993ddf223e07c708ccdb904e6f3e1248364426e102d1a23d9e6213a3beb',
)
KNEWS_TABLE_HTTP_BINDING = (
    '2cb60160b90642dd387f8b3b49b479a92cdb6ca72ec4962102a4f0f392cc74d7',
    'a74543def531ea312d511d3fe462e30b17d64c2a087f2ea922bba3cf830c8de4',
    '4b64e6ca583ff8dadef050f6412bec1f773ed3166da81c489e52f2af154c3080',
    'f7f59993ddf223e07c708ccdb904e6f3e1248364426e102d1a23d9e6213a3beb',
)
KNEWS_FAILURE_ORIGINAL_QUESTION = 'knews_current_native_failed_boundary_original_v1'
KNEWS_HEADER_SHAPE_QUESTION = 'knews_current_native_missing_header_structure_v1'
KNEWS_V2_VERIFICATION = 'native_literal_known_URL_header_schema_distinct_reporter_roles_v2_20261024'
KNEWS_V3_VERIFICATION = 'native_optional_TOC_written_inline_captions_and_quotes_v3_20261024'
KNEWS_V4_VERIFICATION = 'native_declared_desks_profile_slugs_clock_order_and_nested_recommendations_v4_20261024'
KNEWS_V5_VERIFICATION = 'native_title_edges_reporter_slashes_translator_roles_and_complete_short_editor_v5_20261024'
KNEWS_V6_VERIFICATION = 'native_written_cards_nontext_video_mount_and_finance_desk_separators_v6_20261024'
KNEWS_V7_VERIFICATION = 'native_captionless_figures_optional_card_id_and_literal_sparse_signatures_v7_20261024'
KNEWS_V8_VERIFICATION = 'native_written_table_highlights_and_literal_unopened_reporter_signature_v8_20261026'
KNEWS_PATH_PATTERN = r'^/news/[0-9A-F]{32}$'
KNEWS_BODY_SELECTOR = (':scope > figure > figcaption, :scope > p.post-intro, '
                       ':scope > .article-body-wrapper > .article-content-inner')
KNEWS_AUTHOR_SELECTOR = ':scope > .article-meta-panel .abc-byline > a'
KNEWS_EXCLUSIONS = ('.in-article-ad-wrapper', '.related-news-card', '.extend-reading', '.app-promo-wrapper')
KNEWS_REVIEWED_HEADER_AUTHORS = ('芮內', '克里夫')
KNEWS_REVIEWED_URLS = (
    'https://www.knews.com.tw/news/04CE23AF3987F7A667AECC0810CDBF4D',
    'https://www.knews.com.tw/news/F84F5BD478EA9B742886C5819E33B9D7',
)
# Exact reviewed capture/auth -> original HTTP owner. An adapter/scope review
# does not create a new REP/refusal/rate budget for the same public origin.
KNEWS_FINITE_HTTP_BINDING = (
    '83978a41e8b8a02c82c353b66f44a36dd6af87e0acc781a090bc7e96fb740222',
    '7eb32b35ba2cb8fc29e68da4a7ce4cc3545e585e21dad33012931a746b9bf422',
    '4b64e6ca583ff8dadef050f6412bec1f773ed3166da81c489e52f2af154c3080',
    'f7f59993ddf223e07c708ccdb904e6f3e1248364426e102d1a23d9e6213a3beb',
)
# Filled only for the exact reviewed V2 capture/auth and the same origin's
# existing HTTP owner; edits to endpoints, pacing or rights do not match it.
KNEWS_KNOWN_HTTP_BINDING = (
    'ddb33831d1e68c58c427d1c778c6ff7259aae28228b840ce8cef14606b62a211',
    'a74543def531ea312d511d3fe462e30b17d64c2a087f2ea922bba3cf830c8de4',
    '4b64e6ca583ff8dadef050f6412bec1f773ed3166da81c489e52f2af154c3080',
    'f7f59993ddf223e07c708ccdb904e6f3e1248364426e102d1a23d9e6213a3beb',
)
KNEWS_EDITOR_HTTP_BINDING = (
    '53bb3f5be3545d2a50342cd7289dd42507ae351047fa025968ab342f3c4a1036',
    *KNEWS_KNOWN_HTTP_BINDING[1:],
)
KNEWS_DECLARATIONS_HTTP_BINDING = (
    'bde81784a0b60da6fccb90136300640b3eb7ab5f623f91709013af84489e24f1',
    *KNEWS_KNOWN_HTTP_BINDING[1:],
)
KNEWS_CREDITS_HTTP_BINDING = (
    '26c4301bb20a81efb255021f5056128bb9b30e4ff18243ac8fec4c040be1fe99',
    *KNEWS_KNOWN_HTTP_BINDING[1:],
)
KNEWS_WRITTEN_BLOCKS_HTTP_BINDING = (
    'b1e1af2f794ec713cac33cb9a3415f8d57af55142176bc3c04c992890cee60a2',
    *KNEWS_KNOWN_HTTP_BINDING[1:],
)
KNEWS_SPARSE_HTTP_BINDING = (
    'f4df2c87bf598e1c8d49f749d33b1f458f841fcf5db43add98b3bd801159106b',
    *KNEWS_KNOWN_HTTP_BINDING[1:],
)
_ORIGIN = 'https://www.knews.com.tw'
_EXTERNAL_CREDIT = re.compile(r'中央社|中央通訊社|CNA|路透|Reuters|法新社|AFP|美聯社|Associated Press', re.I)
_REPRINT = re.compile(r'^(?:本文|本篇文章)(?:為|由|係)?(?:轉載|轉自)|^(?:全文|原文)轉載', re.M)
_PROSE = frozenset({'p', 'h3', 'br', 'strong', 'b', 'em', 'i', 'u', 's', 'a', 'span',
                    'ul', 'ol', 'li', 'blockquote', 'sub', 'sup', 'figure', 'figcaption', 'img'})


def _clock(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+]08:00', value):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    return value


def _article_link(value):
    parts = urlsplit(value or '')
    return (parts.scheme == 'https' and parts.netloc == 'www.knews.com.tw'
            and re.fullmatch(KNEWS_PATH_PATTERN, parts.path) and not parts.fragment)


def _children(node, *, skip_comments=False):
    for child in node.children:
        if skip_comments and isinstance(child, Comment):
            continue
        if isinstance(child, Tag):
            yield child
        elif str(child).strip():
            raise NewsError('article_identity_not_verified')


def _nested_recommendation(node):
    """Account for every card node/text without dropping a swallowed paragraph."""
    pending, expected = [node], []
    while pending:
        card = pending.pop()
        children = list(_children(card))
        if (card.name != 'div' or card.get('class') != ['related-news-card']
                or card.get('id') not in (None, 'norss') or not children or children[0].name != 'a'
                or any(child.name != 'div' or child.get('class') != ['related-news-card'] for child in children[1:])):
            raise NewsError('article_identity_not_verified')
        link = children[0]
        label, headline = _one(link, 'span'), _one(link, 'h5')
        text = label.get_text(strip=True) + headline.get_text(strip=True)
        if (not _article_link(link.get('href')) or label.get_text(strip=True) != '編輯推薦'
                or not headline.get_text(strip=True) or len(link.select('img')) != 1
                or link.get_text(strip=True) != text
                or any(child.name not in {'div', 'img', 'span', 'h5'} for child in link.find_all())
                or any(str(key).lower().startswith('on') for child in [card, link, *link.find_all()]
                       for key in child.attrs)):
            raise NewsError('article_identity_not_verified')
        expected.append(text)
        pending.extend(reversed(children[1:]))
    if node.get_text(strip=True) != ''.join(expected):
        raise NewsError('article_identity_not_verified')


def _written_cards(root, *, version=6):
    """Verify every header and label/value cell; the text reader keeps them all."""
    if root.name != 'div' or root.attrs != {'class': ['my-3']}:
        raise NewsError('article_identity_not_verified')
    children = list(_children(root, skip_comments=version >= 9))
    if (len(children) != 1 or children[0].name != 'div'
            or (children[0].get('id') != 'norss' and not (version >= 7 and 'id' not in children[0].attrs))
            or set(children[0].attrs) - {'id', 'style'}):
        raise NewsError('article_identity_not_verified')
    wrapper = children[0]
    cards = list(_children(wrapper))
    if not cards:
        raise NewsError('article_identity_not_verified')
    allowed = {id(root), id(wrapper)}
    _visible(root)
    _visible(wrapper)
    for card in cards:
        if card.name != 'div' or set(card.attrs) - {'style'}:
            raise NewsError('article_identity_not_verified')
        rows = list(_children(card))
        if len(rows) < 2:
            raise NewsError('article_identity_not_verified')
        for index, row in enumerate(rows):
            if row.name != 'div' or set(row.attrs) - {'style'}:
                raise NewsError('article_identity_not_verified')
            _visible(row)
            allowed.add(id(row))
            if index == 0:
                if row.find() is not None or not row.get_text(strip=True):
                    raise NewsError('article_identity_not_verified')
                continue
            cells = list(_children(row))
            if len(cells) != 2:
                raise NewsError('article_identity_not_verified')
            for cell in cells:
                if (cell.name != 'span' or set(cell.attrs) - {'style'}
                        or cell.find() is not None or not cell.get_text(strip=True)):
                    raise NewsError('article_identity_not_verified')
                _visible(cell)
                allowed.add(id(cell))
        _visible(card)
        allowed.add(id(card))
    return allowed, len(cards)


def _written_table_highlight(root):
    """Verify the entire native highlight, including its empty accent cell.

    Keep the literal written value in the existing DOM text flow. No table is
    inferred from a partial selector and no prose, resource or unknown control
    can be hidden inside a supposedly decorative cell.
    """
    def one_child(node, name):
        children = list(_children(node))
        if len(children) != 1 or children[0].name != name:
            raise NewsError('article_identity_not_verified')
        return children[0]
    if root.name != 'div' or set(root.attrs) - {'style'}:
        raise NewsError('article_identity_not_verified')
    table = one_child(root, 'table')
    rows = one_child(table, 'tbody')
    row = one_child(rows, 'tr')
    cells = list(_children(row))
    if (len(cells) != 2 or any(cell.name != 'td' or cell.find() is not None for cell in cells)
            or cells[0].get_text(strip=True) or not cells[1].get_text(strip=True)):
        raise NewsError('article_identity_not_verified')
    nodes = [root, table, rows, row, *cells]
    for node in nodes:
        if set(node.attrs) - {'style'}:
            raise NewsError('article_identity_not_verified')
        _visible(node)
    if root.get_text(' ', strip=True) != cells[1].get_text(' ', strip=True):
        raise NewsError('article_identity_not_verified')
    return {id(node) for node in nodes}, len(cells[1].get_text(' ', strip=True))


def _written_data_grid(table):
    """Account for every column, row and literal cell before keeping the table."""
    if table.name != 'table' or set(table.attrs) - {'border', 'style'} or table.get('border') != '1':
        raise NewsError('article_identity_not_verified')
    groups = list(_children(table))
    if len(groups) != 2 or [n.name for n in groups] != ['colgroup', 'tbody']:
        raise NewsError('article_identity_not_verified')
    columns, rows = list(_children(groups[0])), list(_children(groups[1]))
    if (not columns or not rows or any(n.name != 'col' or n.find() is not None
            or n.get_text(strip=True) for n in columns) or any(n.name != 'tr' for n in rows)):
        raise NewsError('article_identity_not_verified')
    nodes = [table, *groups, *columns]
    cells = []
    for row in rows:
        values = list(_children(row))
        if (len(values) != len(columns) or any(n.name != 'td' or n.find() is not None for n in values)
                or not any(n.get_text(strip=True) for n in values)):
            raise NewsError('article_identity_not_verified')
        nodes.extend([row, *values])
        cells.extend(values)
    for node in nodes:
        if set(node.attrs) - ({'border', 'style'} if node is table else {'style'}):
            raise NewsError('article_identity_not_verified')
        _visible(node)
    return {id(n) for n in nodes}, len(rows), len(cells), sum(len(n.get_text(' ', strip=True)) for n in cells)


def _native_hls_video(node, page_url):
    """Keep the existing no-media-transcription boundary; never fetch or execute."""
    keys = {'id', 'class', 'playsinline', 'controls', 'autoplay', 'muted', 'preload',
            'width', 'height', 'poster', 'data-setup'}
    children = list(_children(node))
    if (node.name != 'video' or set(node.attrs) - keys or node.get('id') != 'article-video-hls'
            or node.get('class') != ['vjs-matrix', 'video-js', 'vjs-16-9']
            or node.get_text(strip=True) or len(children) != 1 or children[0].name != 'source'
            or any(node.get(k) != '' for k in ('playsinline', 'controls', 'autoplay', 'muted'))
            or node.get('preload') != 'auto' or node.get('data-setup') != '{"fluid": true,"disablePictureInPicture":true}'
            or any(not re.fullmatch(r'[1-9][0-9]{0,4}', str(node.get(k, ''))) for k in ('width', 'height'))):
        raise NewsError('article_identity_not_verified')
    source = children[0]
    if (set(source.attrs) != {'src', 'type'} or source.get('type') != 'application/x-mpegURL'
            or list(_children(source))):
        raise NewsError('article_identity_not_verified')
    ident = page_url.rsplit('/', 1)[-1]
    prefix = r'https://static\.kmedia\.com\.tw/prd/[0-9]{4}/[0-9]{4}/' + re.escape(ident) + '/'
    if (not re.fullmatch(prefix + r'[0-9a-f]{32}\.webp', str(node.get('poster', '')))
            or not re.fullmatch(prefix + r'vdo/[0-9a-f]{32}elitemedia\.m3u8', str(source.get('src', '')))
            or node['poster'].rsplit('/', 1)[0] != source['src'].rsplit('/vdo/', 1)[0]):
        raise NewsError('article_identity_not_verified')
    _visible(node)
    _visible(source)


def _nontext_video_mount(node, headline):
    """A witnessed unrequested iframe is separate from the written article."""
    if node.name != 'div' or node.attrs != {'class': ['video-container']}:
        raise NewsError('article_identity_not_verified')
    children = list(_children(node))
    if len(children) != 1 or children[0].name != 'iframe':
        raise NewsError('article_identity_not_verified')
    frame = children[0]
    keys = {'allow', 'allowfullscreen', 'frameborder', 'height', 'id', 'referrerpolicy', 'src', 'title', 'width'}
    src = frame.get('src')
    if (set(frame.attrs) - keys or frame.get('id') != 'article-video-youtube'
            or frame.get('title') != headline or not isinstance(src, str)
            or frame.find() is not None or frame.get_text(strip=True)):
        raise NewsError('article_identity_not_verified')
    try:
        parts = urlsplit(src)
    except ValueError:
        raise NewsError('article_identity_not_verified') from None
    query = parse_qsl(parts.query, keep_blank_values=True)
    if (parts.scheme != 'https' or parts.netloc != 'www.youtube.com'
            or not re.fullmatch(r'/embed/[A-Za-z0-9_-]{11}', parts.path) or parts.fragment
            or len(query) != 4 or dict(query) != {
                'autoplay': '1', 'mute': '1', 'playsinline': '1', 'enablejsapi': '1'}
            or any(key in frame.attrs and not re.fullmatch(r'[1-9][0-9]{0,4}', str(frame[key]))
                   for key in ('width', 'height'))):
        raise NewsError('article_identity_not_verified')
    _visible(node)
    _visible(frame)


def _body_structure(scope, intro, body, figure, *, version=1, facts=None):
    """Validate the complete witnessed editor, not an arbitrary selector slice."""
    wrapper = body.parent
    headings = body.select(':scope > h3')
    if version >= 3 and not headings:
        if list(_children(wrapper)) != [body] or wrapper.select('#article-left-toc'):
            raise NewsError('article_identity_not_verified')
    else:
        toc = _one(wrapper, ':scope > #article-left-toc.article-left-toc')
        if list(_children(wrapper)) != [toc, body]:
            raise NewsError('article_identity_not_verified')
        _visible(toc)
        links = toc.select('.toc-list > li > a.toc-link')
        ids = [h.get('id') for h in headings]
        if (not headings or len(links) != len(headings)
                or version >= 4 and (len(set(ids)) != len(ids)
                    or any(not isinstance(value, str) or not re.fullmatch(r'subhead-[0-9]+', value) for value in ids))
                or any((version < 4 and h.get('id') != f'subhead-{index}') or link.get('href') != '#' + str(h.get('id'))
                       or link.get_text(' ', strip=True) != h.get_text(' ', strip=True)
                       for index, (h, link) in enumerate(zip(headings, links)))):
            raise NewsError('article_identity_not_verified')
    excluded, written = set(), set()
    card_count = card_characters = 0
    table_count = table_characters = grid_count = grid_rows = grid_cells = card_comments = 0
    for node in _children(body):
        classes = node.get('class', [])
        if classes == ['in-article-ad-wrapper']:
            if (node.name != 'div' or node.get_text(strip=True)
                    or any(child.name != 'div' for child in node.find_all())):
                raise NewsError('article_identity_not_verified')
        elif classes == ['related-news-card']:
            if version >= 4:
                _nested_recommendation(node)
                excluded.add(id(node))
                continue
            link = _one(node, ':scope > a')
            label, headline = _one(link, 'span'), _one(link, 'h5')
            if (node.name != 'div' or node.get('id') != 'norss'
                    or not _article_link(link.get('href'))
                    or list(_children(node)) != [link]
                    or label.get_text(strip=True) != '編輯推薦' or len(link.select('img')) != 1
                    or node.get_text(strip=True) != label.get_text(strip=True) + headline.get_text(strip=True)
                    or any(child.name not in {'a', 'div', 'img', 'span', 'h5'} for child in node.find_all())):
                raise NewsError('article_identity_not_verified')
        elif classes == ['extend-reading']:
            if node.name != 'p':
                raise NewsError('article_identity_not_verified')
            if node.get_text(strip=True):
                if (_one(node, ':scope > span').get_text(' ', strip=True) != '🔗 延伸閱讀'
                        or not _article_link(_one(node, ':scope > a').get('href'))
                        or [c.name for c in _children(node)] != ['span', 'a']):
                    raise NewsError('article_identity_not_verified')
            elif list(_children(node)):
                raise NewsError('article_identity_not_verified')
        elif classes == ['app-promo-wrapper']:
            link = _one(node, ':scope > a.app-promo-link')
            if (node.name != 'p' or link.get('href') != 'https://event.knews.com.tw/app'
                    or list(_children(node)) != [link]
                    or link.find() is not None
                    or link.get_text(' ', strip=True) != '⭐️ 即刻下載！無廣告《知新聞》App'):
                raise NewsError('article_identity_not_verified')
        elif version >= 8 and node.name == 'div' and not classes and node.select(':scope > table'):
            nodes, characters = _written_table_highlight(node)
            written.update(nodes)
            table_count += 1
            table_characters += characters
            continue
        elif version >= 9 and node.name == 'table':
            nodes, rows, cells, characters = _written_data_grid(node)
            written.update(nodes)
            grid_count += 1
            grid_rows += rows
            grid_cells += cells
            table_characters += characters
            continue
        elif version >= 6 and node.name == 'div' and classes == ['my-3']:
            nodes, count = _written_cards(node, version=version)
            if version >= 9:
                card_comments += sum(isinstance(child, Comment) for child in node.children)
            written.update(nodes)
            card_count += count
            card_characters += len(node.get_text(' ', strip=True))
            continue
        else:
            prose_tags = {'p', 'h3', 'blockquote', 'figure'} if version >= 3 else {'p', 'h3'}
            if node.name not in prose_tags or classes:
                raise NewsError('article_identity_not_verified')
            continue
        excluded.add(id(node))
    for container in (intro, body, figure):
        _visible(container)
        for node in container.find_all():
            if id(node) in excluded or any(id(parent) in excluded for parent in node.parents):
                continue
            if id(node) in written:
                continue  # Every header and cell was checked above and remains in the text flow.
            if node.name not in _PROSE and not (version >= 3 and node.name == 'blockquote'):
                raise NewsError('article_identity_not_verified')
            _visible(node)
    _visible(_one(figure, ':scope > figcaption'))
    # The shared text contract omits figures. V3 verifies an entire native
    # written figure before lifting its one caption into the prose flow.
    # Images/alt/data-glightbox are never text, downloads or transcriptions.
    captionless_figures = 0
    for node in body.select('figure'):
        if not node.get_text(strip=True):
            if version >= 3 and node.parent is body:
                if version < 7:
                    raise NewsError('article_identity_not_verified')
            else:
                continue
        if version < 3 or node.parent is not body or node.attrs:
            raise NewsError('article_identity_not_verified')
        link, caption = _one(node, ':scope > a'), _one(node, ':scope > figcaption')
        if (list(_children(node)) != [link, caption] or not link.find('img')
                or any(child.name != 'img' for child in link.find_all())
                or link.get_text(strip=True) or version < 7 and not caption.get_text(strip=True)
                or node.get_text(' ', strip=True) != caption.get_text(' ', strip=True)
                or any(str(key).lower().startswith('on') for child in [node, *node.find_all()]
                       for key in child.attrs)):
            raise NewsError('article_identity_not_verified')
        if not caption.get_text(strip=True):
            captionless_figures += 1
    # A visible introduction/schema description alone is not the complete story.
    prose = [n for n in body.select(':scope > p') if id(n) not in excluded]
    prose_characters = sum(len(n.get_text(' ', strip=True)) for n in prose) + card_characters + table_characters
    if prose_characters < (1 if version >= 5 else 100):
        raise NewsError('body_not_found')
    expected = {'h1', 'h2', 'figure', 'p', 'div', 'script', 'style'}
    if version >= 9:
        expected.add('video')
    video_mounts = 0
    for node in _children(scope):
        if node.name not in expected:
            raise NewsError('article_identity_not_verified')
        if node.name == 'h2' and node.get_text(strip=True):
            raise NewsError('article_identity_not_verified')
        if node.name == 'p' and node is not intro:
            raise NewsError('article_identity_not_verified')
        if version >= 6 and node.name == 'div' and node.get('class') == ['video-container']:
            _nontext_video_mount(node, _one(scope, ':scope > h1').get_text(' ', strip=True))
            video_mounts += 1
            continue
        if node.name == 'div' and node is not wrapper and not (
                node.get('id') == 'adm-wrapper' and node.get('class') == ['ad']
                or node.get('class') in (['knews-ticker-root'], ['article-meta-panel', 'abc-style'],
                                         ['post-share', 'sharetop'])):
            raise NewsError('article_identity_not_verified')
    if facts is not None:
        if grid_count:
            facts.update(native_written_data_grids=grid_count, native_written_data_rows=grid_rows,
                native_written_data_cells=grid_cells, native_written_grid_values_retained=True)
        if card_comments:
            facts.update(native_inert_card_comments_excluded=card_comments, commented_text_not_article_prose=True)
        if table_count:
            facts.update(native_written_table_highlights=table_count,
                native_written_table_characters=table_characters, native_written_table_values_retained=True)
        if captionless_figures:
            facts['native_captionless_figures_verified'] = captionless_figures
        if card_count:
            facts.update(native_written_cards=card_count, native_written_card_characters=card_characters,
                native_written_card_labels_and_values_retained=True)
        if video_mounts:
            facts.update(native_nontext_video_mounts_verified=video_mounts,
                video_requested_or_transcribed=False)
    return prose_characters


def _native_identity(soup, *, page_url, version=1):
    if type(version) is not int or version not in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10):
        raise ValueError('unreviewed Knews native version')
    if (not isinstance(page_url, str)
            or not re.fullmatch(re.escape(_ORIGIN) + r'/news/[0-9A-F]{32}', page_url)
            or version == 1 and page_url not in KNEWS_REVIEWED_URLS):
        raise NewsError('article_identity_not_verified')
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _one(soup, 'head > meta[property="og:url"]').get('content') != page_url):
        raise NewsError('article_identity_not_verified')
    articles = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            articles.extend(obj for obj in _objects(json.loads(script.get_text()))
                            if obj.get('@type') == 'NewsArticle')
        except (ValueError, TypeError, RecursionError):
            raise NewsError('article_identity_not_verified') from None
    if len(articles) != 1:
        raise NewsError('article_identity_not_verified')
    declared, scope = articles[0], _one(soup, 'article#post-detail')
    _visible(scope)
    if 'isAccessibleForFree' in declared and declared['isAccessibleForFree'] is not True:
        raise NewsError('paywall_or_login')
    if (declared.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': page_url}
            or not isinstance(declared.get('publisher'), dict)
            or declared.get('publisher', {}).get('@type') != 'Organization'
            or declared['publisher'].get('name') != '知新聞'):
        raise NewsError('article_identity_not_verified')
    title = _one(scope, ':scope > h1')
    _visible(title)
    headline = title.get_text(' ', strip=True)
    schema_headline = declared.get('headline')
    og_title = _one(soup, 'head > meta[property="og:title"]').get('content')
    if (not headline or not isinstance(schema_headline, str) or not isinstance(og_title, str)
            or (version < 5 and (headline != schema_headline or og_title != headline + '｜知新聞'))
            or (version >= 5 and (headline != schema_headline.strip() or not og_title.endswith('｜知新聞')
                                 or og_title.removesuffix('｜知新聞').strip() != headline))):
        raise NewsError('article_identity_not_verified')
    return scope, declared, headline, schema_headline, og_title


def _multiple_native_reporters(soup, scope, declared, *, facts):
    """Keep each literal Person/profile and source order; no inferred aliases."""
    byline = _one(scope, ':scope > .article-meta-panel .abc-byline')
    _visible(byline)
    children = list(_children(byline))
    if (not children or children[0].name != 'span' or children[0].get('class') != ['label']
            or children[0].get_text(strip=True) != '記者'
            or children[0].find() is not None or set(children[0].attrs) - {'class', 'style'}
            or set(byline.attrs) - {'class', 'style'}):
        raise NewsError('article_attribution_not_verified')
    _visible(children[0])
    reporters, separators = [], []
    for index, node in enumerate(children[1:], 1):
        _visible(node)
        if node.find() is not None or any(str(key).lower().startswith('on') for key in node.attrs):
            raise NewsError('article_attribution_not_verified')
        if node.name == 'a':
            name, profile = node.get_text(' ', strip=True), node.get('href')
            if (set(node.attrs) - {'href', 'class', 'style', 'rel', 'target'}
                    or not name or len(name) > 40 or not isinstance(profile, str) or len(profile) > len(_ORIGIN) + 90
                    or not re.fullmatch(re.escape(_ORIGIN) + r'/reporter/[a-z0-9]+(?:[._-][a-z0-9]+)*', profile)):
                raise NewsError('article_attribution_not_verified')
            reporters.append({'@type': 'Person', 'name': name, 'url': profile})
        elif (node.name == 'span' and not set(node.attrs) - {'class', 'style'}
                and node.get_text(strip=True) in {'、', ',', '，', '/', '／'}
                and index > 1 and index + 1 < len(children)
                and children[index-1].name == children[index+1].name == 'a'):
            separators.append(node.get_text())
        else:
            raise NewsError('article_attribution_not_verified')
    names = [r['name'] for r in reporters]
    meta_raw = _one(soup, 'head > meta[name=author]').get('content')
    if (not 2 <= len(reporters) <= 8 or len({r['url'] for r in reporters}) != len(reporters)
            or declared.get('author') != reporters or not isinstance(meta_raw, str) or len(meta_raw) > 360
            or re.split(r'\s*[、,，／/]\s*', meta_raw.strip()) != names):
        raise NewsError('article_attribution_not_verified')
    facts.update(reporters=reporters, source_meta_author_raw=meta_raw,
        source_header_credit_raw=byline.get_text(' ', strip=True), source_header_separators_raw=separators)
    return meta_raw, None


def _native_credit(soup, scope, declared, *, version, facts=None):
    if version >= 10:
        credits = scope.select(KNEWS_AUTHOR_SELECTOR)
        if len(credits) != 1:
            return _multiple_native_reporters(soup, scope, declared, facts=facts if facts is not None else {})
        credit = credits[0]
    else:
        credit = _one(scope, KNEWS_AUTHOR_SELECTOR)
    _visible(credit)
    name, profile = credit.get_text(' ', strip=True), credit.get('href')
    profile_pattern = r'[a-z0-9]+(?:[._-][a-z0-9]+)*' if version >= 4 else r'[a-z]+'
    if (_one(credit.parent, ':scope > span.label').get_text(strip=True) != '記者'
            or not name or len(name) > 40
            or version == 1 and name not in KNEWS_REVIEWED_HEADER_AUTHORS
            or not isinstance(profile, str) or version >= 4 and len(profile) > len(_ORIGIN) + 90
            or not re.fullmatch(re.escape(_ORIGIN) + r'/reporter/' + profile_pattern, profile)
            or _one(soup, 'head > meta[name=author]').get('content') != name
            or declared.get('author') != [{'@type': 'Person', 'name': name, 'url': profile}]):
        raise NewsError('article_attribution_not_verified')
    return name, profile


def _native_clocks(scope, declared, *, version):
    published, modified = _clock(declared.get('datePublished')), _clock(declared.get('dateModified'))
    timestamp = _one(scope, ':scope > .article-meta-panel .abc-timestamp')
    visible_clock = _one(timestamp, ':scope > time')
    _visible(visible_clock)
    visible_raw = visible_clock.get_text(' ', strip=True)
    modified_precedes_publication = datetime.fromisoformat(modified) < datetime.fromisoformat(published)
    if (_one(timestamp, ':scope > span.label').get_text(strip=True) != '發布'
            or visible_raw != datetime.fromisoformat(published).strftime('%Y/%m/%d %H:%M')
            or version < 4 and modified_precedes_publication):
        raise NewsError('invalid_bulk_publication_clock')
    return published, modified, visible_raw, modified_precedes_publication


def _native_frame(soup, *, page_url, version=1, credit_facts=None):
    scope, declared, headline, schema_headline, og_title = _native_identity(
        soup, page_url=page_url, version=version)
    name, profile = _native_credit(soup, scope, declared, version=version, facts=credit_facts)
    published, modified, visible_raw, modified_precedes_publication = _native_clocks(
        scope, declared, version=version)
    return (scope, headline, name, profile, published, modified, visible_raw,
            modified_precedes_publication, schema_headline, og_title)


def knews_missing_header_metadata(response, source, requested_url):
    """Bounded structure and equality facts, never names, prose or raw admission."""
    from bs4 import Tag
    from downloader.news_fulltext_policy import public_article_document
    if (not knews_resident_source(source) or source.embedded_article_layout != KNEWS_GRID_NATIVE_LAYOUT
            or response.status != 200 or response.url != requested_url or response.redirects
            or not 0 < len(response.body) <= 2 << 20):
        raise NewsError('knews_header_diagnostic_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=requested_url)
    scope, declared, _, _, _ = _native_identity(soup, page_url=requested_url, version=9)
    _native_clocks(scope, declared, version=9)
    panels = scope.select(':scope > .article-meta-panel')
    if len(panels) != 1:
        raise NewsError('knews_header_diagnostic_scope_not_verified')
    bylines = panels[0].select('.abc-byline')
    declared_authors = declared.get('author')
    if not isinstance(declared_authors, list) or len(declared_authors) > 8:
        raise NewsError('knews_header_diagnostic_scope_not_verified')
    metas = soup.select('head > meta[name=author]')
    meta_name = metas[0].get('content') if len(metas) == 1 else None
    names = [a.get('name') for a in declared_authors if isinstance(a, dict) and isinstance(a.get('name'), str)]
    profiles = [a.get('url') for a in declared_authors if isinstance(a, dict) and isinstance(a.get('url'), str)]
    elements = list(panels[0].descendants)
    if len(elements) > 96 or len(bylines) > 4:
        raise NewsError('knews_header_diagnostic_scope_not_verified')
    shape = []
    for node in elements:
        if not isinstance(node, Tag):
            continue
        try:
            _visible(node)
        except NewsError:
            shape.append({'tag': node.name, 'visible': False})
            continue
        text = node.get_text(' ', strip=True)
        common_attributes = {'class', 'id', 'style', 'href', 'datetime', 'title', 'target', 'rel'}
        shape.append({'tag': node.name if node.name in {'div', 'span', 'a', 'time', 'p', 'strong', 'b', 'small', 'svg', 'path'} else 'other',
            'visible': True, 'children': len(node.find_all(recursive=False)),
            'attribute_names': sorted(set(node.attrs) & common_attributes),
            'unknown_attribute_count': len(set(node.attrs) - common_attributes),
            'native_byline': node in bylines, 'native_label': 'label' in node.get('class', []),
            'written_characters': len(text), 'label_is_reporter': text == '記者',
            'equals_meta_author': bool(text) and text == meta_name,
            'equals_declared_author': bool(text) and text in names,
            'href_equals_declared_profile': bool(node.get('href')) and node.get('href') in profiles})
    return {'contract_version': 1, 'native_literal_identity_and_clock_verified': True,
        'native_anchor_count': len(scope.select(KNEWS_AUTHOR_SELECTOR)), 'native_byline_count': len(bylines),
        'meta_author_count': len(metas), 'schema_author_count': len(declared_authors),
        'schema_person_count': sum(isinstance(a, dict) and a.get('@type') == 'Person' for a in declared_authors),
        'schema_organization_count': sum(isinstance(a, dict) and a.get('@type') == 'Organization' for a in declared_authors),
        'meta_author_equals_one_declared_name': isinstance(meta_name, str) and meta_name in names,
        'meta_or_schema_external_credit': any(_EXTERNAL_CREDIT.search(n) for n in [meta_name, *names] if isinstance(n, str)),
        'native_panel_shape': shape, 'names_or_written_values_retained': False,
        'raw_retention_permitted': False, 'body_or_identity_accepted': False}


def knews_resident_source(source):
    """The current reviewed origin; diagnosis does not grant another source."""
    return bool(source and source.provider == 'knews' and source.allowed and source.request_enabled
        and source.known_url_only and source.acquisition_mode == 'continuous'
        and source.embedded_article_layout in {KNEWS_SPARSE_NATIVE_LAYOUT, KNEWS_TABLE_NATIVE_LAYOUT, KNEWS_GRID_NATIVE_LAYOUT, KNEWS_MULTI_REPORTER_LAYOUT}
        and source.domains == ('www.knews.com.tw',)
        and source.selectors == (KNEWS_BODY_SELECTOR,)
        and source.authorization.get('kind') == 'personal_research'
        and source.authorization.get('evidence_url') == _ORIGIN + '/')


def private_knews_failure_original_permit(response, source, requested_url):
    """Retain an already received original, without accepting its failed body.

    The signed header, literal document and declared clocks still have to pass.
    Unknown body geometry and printed signatures are the diagnostic question;
    they cannot supply author identity, a publication clock or a body approval.
    No embedded resource is requested or executed.
    """
    import hashlib
    from downloader.news_fulltext_policy import public_article_document
    if (not knews_resident_source(source) or response.status != 200
            or response.url != requested_url or response.redirects
            or not 0 < len(response.body) <= 2 << 20
            or not re.search(r'(?:text/html|application/xhtml[+]xml)',
                             response.headers.get('content-type', ''), re.I)):
        raise NewsError('private_knews_original_scope_not_verified')
    soup = public_article_document(response.body, source, page_url=response.url)
    scope, _, name, _, _, _, _, _, _, _ = _native_frame(soup, page_url=response.url,
        version=10 if source.embedded_article_layout == KNEWS_MULTI_REPORTER_LAYOUT else 7)
    intro = _one(scope, ':scope > p.post-intro')
    body = _one(scope, ':scope > .article-body-wrapper > .article-content-inner')
    _visible(intro)
    _visible(body)
    if (_EXTERNAL_CREDIT.search(name) or _EXTERNAL_CREDIT.search(intro.get_text(' ', strip=True))
            or _REPRINT.search(intro.get_text('\n', strip=True))
            or _REPRINT.search(body.get_text('\n', strip=True))):
        raise NewsError('article_rights_or_scope_excluded')
    return {'contract_version': 1, 'question': KNEWS_FAILURE_ORIGINAL_QUESTION,
        'provider': source.provider, 'url': response.url,
        'response_sha256': hashlib.sha256(response.body).hexdigest(),
        'data_fingerprint': source.data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint,
        'scope': 'reviewed_current_public_native_private_failed_boundary_original_only',
        'literal_header_and_clock_verified': True, 'body_geometry_accepted': False,
        'printed_signature_accepted': False, 'article_accepted': False}


def knews_public_native_article(soup, *, page_url, version=1):
    credit_facts = {}
    (scope, headline, name, profile, published, modified, visible_raw,
     modified_precedes_publication, schema_headline, og_title) = _native_frame(
        soup, page_url=page_url, version=version, credit_facts=credit_facts)
    intro = _one(scope, ':scope > p.post-intro')
    body = _one(scope, ':scope > .article-body-wrapper > .article-content-inner')
    figure = _one(scope, ':scope > figure')
    intro_text = intro.get_text(' ', strip=True)
    reporter_pattern = (r'^【記者([^／╱\s【】]{0,30})[／╱]([^【】]{1,30})報導】' if version >= 7 else
                        r'^【記者([^／╱\s【】]{1,30})[／╱]([^【】]{1,30})報導】' if version >= 5
                        else r'^【記者([^／\s【】]{1,30})／([^【】]{1,30})報導】')
    printed = re.match(reporter_pattern, intro_text)
    unopened_signature = (re.match(r'^記者([^／╱\s【】]{1,30})[／╱]([^【】]{1,30})報導】', intro_text)
                          if version >= 8 and printed is None else None)
    printed = printed or unopened_signature
    desk_pattern = r'^【(財經中心)[／╱]([^【】]{1,30})報導】' if version >= 6 else r'^【(財經中心)╱([^【】]{1,30})報導】'
    desk = re.match(desk_pattern, intro_text) if version >= 4 else None
    translator = re.match(r'^【編譯([^／╱\s【】]{1,30})[／╱]([^【】]{1,30})】', intro_text) if version >= 5 else None
    untyped = (re.match(r'^【([^／╱\s【】]{1,30})[／╱]([^【】]{1,30})報導】', intro_text)
               if version >= 7 and not (printed or desk or translator) else None)
    printed = printed or desk or translator or untyped
    if printed is None:
        raise NewsError('article_attribution_not_verified')
    unnamed_reporter = version >= 7 and not printed[1]
    if version >= 2 and (_EXTERNAL_CREDIT.search(name) or _EXTERNAL_CREDIT.search(printed[1])
                         or _REPRINT.search(intro.get_text('\n', strip=True))
                         or _REPRINT.search(body.get_text('\n', strip=True))):
        raise NewsError('article_rights_or_scope_excluded')
    body_facts = {}
    if version >= 9:
        videos = scope.select(':scope > video')
        for video in videos:
            _native_hls_video(video, page_url)
        if videos:
            body_facts.update(native_nontext_HLS_videos_verified=len(videos),
                HLS_or_poster_requested_or_transcribed=False)
    editor_characters = _body_structure(scope, intro, body, figure, version=version, facts=body_facts)
    if version >= 3:
        for written in list(body.select(':scope > figure')):
            written.replace_with(written.select_one(':scope > figcaption'))
    detail = {'contract_version': version, 'source_header_credit': name, 'literal_profile_url': profile,
        'source_schema_author_name': name, 'source_schema_author_type_raw': 'Person',
        'printed_byline_raw': printed[0],
        'printed_reporter_name': None if desk or translator or untyped or unnamed_reporter else printed[1],
        'printed_reporting_location': printed[2],
        'header_and_printed_names_equal': False if desk or translator else name == printed[1],
        'different_names_are_verified_aliases': False, 'human_reporter_identity_verified': False,
        'original_ownership_verified': False,
        'source_credit_roles': ['publisher_current_header_credit',
            'source_printed_editorial_desk_signature' if desk else 'source_printed_translator_signature'
            if translator else 'source_printed_untyped_signature' if untyped else
            'source_printed_unnamed_reporter_signature' if unnamed_reporter else 'source_printed_reporter_signature']}
    if credit_facts:
        detail.update(source_header_credit=None, source_schema_author_name=None,
            source_header_credits=credit_facts['reporters'], source_schema_authors_raw=credit_facts['reporters'],
            source_header_reporter_order_verified=True,
            **{k:v for k,v in credit_facts.items() if k != 'reporters'})
    if desk:
        detail.update(printed_editorial_desk_name=desk[1], printed_credit_role='editorial_desk')
    if translator:
        detail.update(printed_translator_name=translator[1], printed_credit_role='translator')
    if untyped:
        detail.update(printed_credit_name=untyped[1], printed_credit_role='unspecified')
    if unnamed_reporter:
        detail.update(printed_credit_role='unnamed_reporter', printed_reporter_name_declared=False)
    if unopened_signature:
        detail.update(printed_signature_opening_bracket_present=False, source_signature_text_rewritten=False)
    notices = {'contract_version': version, 'visible_publication_clock_raw': visible_raw,
        'visible_publication_precision': 'minute', 'schema_publication_clock_raw': published,
        'schema_modified_clock_raw': modified, 'schema_clock_precision': 'second',
        'schema_timezone_offset_raw': '+08:00', 'visible_timezone_declared': False,
        'historical_first_release_verified': False, 'schema_description_used_as_full_body': False,
        'media_or_scripts_requested_or_executed': False}
    notices.update(body_facts)
    if version >= 5 and (schema_headline != headline or og_title != headline + '｜知新聞'):
        notices.update(source_schema_headline_raw=schema_headline, source_og_title_raw=og_title,
            title_edge_whitespace_used_for_comparison=True, internal_title_characters_rewritten=False)
    if version >= 5 and editor_characters < 100:
        notices.update(short_native_editor_characters=editor_characters,
            short_native_editor_beyond_introduction_verified=True, schema_description_used_as_full_body=False)
    if modified_precedes_publication:
        notices.update(schema_modified_precedes_publication=True,
            source_modified_clock_rewritten=False,
            modified_clock_order_used_to_infer_publication=False)
    return {'headline': headline, 'author': ([{'name': r['name']} for r in credit_facts['reporters']]
        if credit_facts else [{'name': name}]), 'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'publisher_schema_declared_seconds_offset_matching_visible_minute; no_historical_first_release_proof',
        'article_identity_basis': ('literal_reviewed_URL_canonical_og_schema_headline_native_editor_and_distinct_credits_v1'
            if version == 1 else f'literal_known_URL_canonical_og_schema_headline_native_editor_and_distinct_credits_v{version}'),
        'author_attribution_detail_json': json.dumps(detail, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': ('whole_visible_main_caption_intro_and_editor_prose_in_source_order_once; toc_related_app_ads_excluded; no_media_transcription'
            if version < 3 else 'whole_visible_main_and_inline_captions_intro_quotations_and_editor_prose_in_source_order_once; optional_toc_related_app_ads_excluded; no_media_transcription')}, scope
