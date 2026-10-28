"""Two reviewed, public nStock announcement pages in the canonical collector.

This reader never executes source JavaScript or makes requests. The source's
literal Nuxt object is admitted only when its ID, visible heading, visible
byline, second-precision offset clock, issuer and native announcement key all
agree. It is an nStock representation, not a verified MOPS or CNYES original.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Comment, NavigableString, Tag

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


NSTOCK_LAYOUT = 'nstock_finite_announcement_v1'
NSTOCK_PATH_PATTERN = r'^/news/article_m$'
NSTOCK_BODY_SELECTOR = ':scope > .nstock-native-announcement'
NSTOCK_REVIEWED_URLS = (
    'https://www.nstock.tw/news/article_m?id=169138',
    'https://www.nstock.tw/news/article_m?id=455676',
)
_KEYS = frozenset({'id', 'title', 'img', 'category', 'pubDate', 'author',
                   'summary', 'content', 'stock', 'keyword', 'insteadKey'})
_DIV_STYLE = ('display:inline-block;background-color: #eeeeee;padding:0.2rem;'
              'padding-right:0.7rem;margin:-0.2rem;padding-left:0.7rem;'
              'border: 0.5px solid #9e9e9e')
_HR_STYLE = 'height:0.7rem;border:0'


def nstock_article_id(url):
    """Recognize only the exact observed requests, including the query."""
    if url not in NSTOCK_REVIEWED_URLS:
        return None
    parts = urlsplit(url)
    return parts.query.removeprefix('id=')


def _native_object(soup):
    scripts = [script.get_text() for script in soup.find_all('script')
               if 'window.__NUXT__=' in script.get_text()]
    if len(scripts) != 1:
        raise NewsError('article_identity_not_verified')
    text = scripts[0]
    markers = list(re.finditer(r'(?<![A-Za-z0-9_$])article:\{', text))
    if len(markers) != 1:
        raise NewsError('article_identity_not_verified')
    cursor = markers[0].end()
    decoder = json.JSONDecoder()
    fields = {}
    while True:
        key = re.match(r'[A-Za-z][A-Za-z0-9]*:', text[cursor:])
        if key is None:
            raise NewsError('article_identity_not_verified')
        name = key.group()[:-1]
        if name in fields or name not in _KEYS:
            raise NewsError('article_identity_not_verified')
        cursor += len(key.group())
        if cursor < len(text) and text[cursor] == '"':
            try:
                value, cursor = decoder.raw_decode(text, cursor)
                if not isinstance(value, str):
                    raise ValueError('not a literal string')
                value.encode('utf-8')  # Reject unpaired surrogate escapes.
            except (ValueError, UnicodeEncodeError):
                raise NewsError('article_identity_not_verified') from None
        elif name in {'summary', 'keyword'}:
            # Aliases are unused display metadata; never resolve JS variables.
            alias = re.match(r'[A-Za-z_$][A-Za-z0-9_$]*', text[cursor:])
            if alias is None:
                raise NewsError('article_identity_not_verified')
            value = None
            cursor += len(alias.group())
        else:
            raise NewsError('article_identity_not_verified')
        fields[name] = value
        if cursor >= len(text) or text[cursor] not in ',}':
            raise NewsError('article_identity_not_verified')
        if text[cursor] == '}':
            break
        cursor += 1
    if set(fields) != _KEYS:
        raise NewsError('article_identity_not_verified')
    return fields


def _verified_fragment(fragment):
    """Accept only the observed public plain-text announcement presentation."""
    if not fragment or '\x00' in fragment:
        raise NewsError('body_not_found')
    tree = BeautifulSoup(fragment, 'html.parser')
    if any(isinstance(node, Comment) for node in tree.descendants):
        raise NewsError('article_identity_not_verified')
    for node in tree.descendants:
        if isinstance(node, Tag):
            if node.name == 'hr':
                valid = node.attrs == {'style': _HR_STYLE} and not node.contents
            elif node.name == 'div':
                valid = (node.attrs == {'style': _DIV_STYLE}
                         and all(isinstance(child, NavigableString) and not isinstance(child, Comment)
                                 for child in node.children))
            else:
                valid = False
            if not valid or node.parent is not tree:
                raise NewsError('article_identity_not_verified')
        elif not isinstance(node, NavigableString):
            raise NewsError('article_identity_not_verified')
    if len(tree.get_text(' ', strip=True)) < 100:
        raise NewsError('body_not_found')
    return fragment


def nstock_finite_announcement(soup, *, page_url):
    identifier = nstock_article_id(page_url)
    if identifier is None:
        raise NewsError('article_identity_not_verified')
    native = _native_object(soup)
    headline = _one(soup, 'h1')
    clock = _one(soup, 'time')
    bylines = [node for node in soup.select('div.text-gray-500')
               if node.get_text(' ', strip=True).startswith('作者：')]
    if len(bylines) != 1:
        raise NewsError('article_attribution_not_verified')
    byline = bylines[0]
    for node in (headline, clock, byline):
        _visible(node)
    title = headline.get_text(' ', strip=True)
    if (native['id'] != identifier or native['title'] != title
            or native['category'] != '公司公告查詢' or native['author'] != 'nStock快訊'
            or byline.get_text(' ', strip=True) != '作者：nStock快訊'):
        raise NewsError('article_identity_not_verified')
    try:
        instant = datetime.fromisoformat(str(clock.get('datetime', '')))
        source_clock = instant.strftime('%Y-%m-%d %H:%M:%S')
        if (not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00',
                             str(clock.get('datetime', '')))
                or native['pubDate'] != source_clock
                or clock.get_text(' ', strip=True) != '發布時間：' + source_clock):
            raise ValueError('conflicting source clocks')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    stock = re.fullmatch(r'([0-9]{4,6})\(TW\)', native['stock'])
    subject = re.fullmatch(r'【公告】[^：]+：(.+)', title)
    if stock is None or subject is None:
        raise NewsError('article_identity_not_verified')
    expected_key = ('mops解析公告查詢_' + source_clock[:10].replace('-', '') + '_'
                    + stock[1] + '_' + subject[1])
    if native['insteadKey'] != expected_key:
        raise NewsError('article_identity_not_verified')
    fragment = _verified_fragment(native['content'])
    return {
        'headline': title,
        'author': [{'name': 'nStock快訊'}],
        'author_attribution_detail_json': json.dumps({
            'source_declared_desk': 'nStock快訊', 'named_person_author': None,
            'source_native_announcement_key': native['insteadKey'],
            'claimed_MOPS_origin_not_independently_verified': True,
        }, ensure_ascii=False),
        'datePublished': clock['datetime'],
        'published_time_basis': 'nstock_visible_datetime_explicit_offset_and_matching_declared_seconds; not_verified_MOPS_or_CNYES_release',
        'article_identity_basis': 'literal_finite_url_Nuxt_id_visible_heading_clock_desk_stock_and_announcement_key_v1',
        'body_completeness': 'current_public_Nuxt_announcement_text; one_retained_response_render_parity_reviewed; upstream_original_not_verified',
        'articleBody': fragment,
    }, soup
