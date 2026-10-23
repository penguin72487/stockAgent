"""Pure, finite wealth-page identity adapter; no browser, API or media requests.

Only the reviewed wealth host/layout is supported. A query ID is part of the
literal identity, the visible byline must agree with the page's declared writer,
and a visible publication day remains a day, not a fabricated midnight clock.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _one, _visible


WEALTH_LAYOUT = 'businessweekly_wealth_public_written_v1'
WEALTH_ORIGIN = 'https://wealth.businessweekly.com.tw'
WEALTH_PATH_PATTERN = r'^/GArticle[.]aspx$'
WEALTH_BODY_SELECTOR = ':scope > .article_main'
WEALTH_AUTHOR_SELECTOR = ':scope > .articlefont.smart > .article_date > a.write > span'
WEALTH_EXCLUSIONS = ('adcover', 'ins', '.article_kv')
# This exact one-original admission retains the earlier independent wealth
# diagnostic request/refusal identity. A parser or progress-page change must
# not create a new source budget; no main/Smart result or another URL inherits it.
WEALTH_FINITE_HTTP_BINDING = (
    '71188c0855914a7702697dbaf5c1fa1229e6a816ad146d0c177c926e5030787c',
    '0f9867ffd1ecc62078cb1b5f95b9c473cc88a310f3300cf22a2cb987f20fa6a1',
    '45142ad02d9b4d022ae41099c79d99da34d015f1204c9af8eab500a8c8ed7ac9',
    'bc2edf74da04ec097177a321b7559d2cabe9d53467dce21a6c46236dc69c2fe9',
)
_WRITTEN_TAGS = frozenset({'div', 'p', 'h2', 'h3', 'h4', 'a', 'br', 'span',
    'strong', 'b', 'em', 'i', 'u', 's', 'sub', 'sup', 'font', 'ul', 'ol', 'li',
    'table', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'caption'})


def _hidden_declaration(soup, identifier):
    node = _one(soup, '#'+identifier)
    if node.name != 'input' or node.get('type') != 'hidden':
        raise NewsError('article_identity_not_verified')
    return node.get('value')


def _single_page(scope, page_url):
    pagination = _one(scope, ':scope > .pagearea')
    _visible(pagination)
    listing = _one(pagination, ':scope > ul')
    items = listing.find_all(recursive=False)
    expected_classes = [['first'], ['prepage'], None, ['nextpage'], ['last'], ['goback'], ['single']]
    if (len(items) != 7 or any(n.name != 'li' or n.get('class') != classes
            for n, classes in zip(items, expected_classes))):
        raise NewsError('article_body_incomplete')
    for item, label in zip(items[:5], ('第一頁', '«', '1', '»', '最後頁')):
        child = _one(item, ':scope > span')
        _visible(child)
        if (item.find_all(recursive=False) != [child] or child.find_all()
                or child.get_text(strip=True) != label
                or label == '1' and child.get('class') != ['select']):
            raise NewsError('article_body_incomplete')
    for item, target, label in ((items[5], 'index.aspx', '首頁'),
            (items[6], page_url+'&p=0', '單頁閱讀')):
        child = _one(item, ':scope > a')
        _visible(child)
        if (item.find_all(recursive=False) != [child] or child.find_all()
                or child.get('href') != target or child.get_text(strip=True) != label):
            raise NewsError('article_body_incomplete')
    # The unused single-page link is evidence, not a generated follow-up GET.
    return page_url+'&p=0'


def _media_only(node):
    """Do not delete a caption, chart value or prose under a media class."""
    children = node.find_all(recursive=False)
    if (node.name != 'div' or len(children) != 2 or children[0].name != 'a'
            or children[1].name != 'div' or children[1].get('class') != ['imgtxt_wrp']):
        raise NewsError('article_identity_not_verified')
    image = _one(children[0], ':scope > img')
    wrapper = children[1]
    caption = _one(wrapper, ':scope > .imgtxt')
    zoom = _one(wrapper, ':scope > a')
    label = _one(zoom, ':scope > .img-zoom')
    target = children[0].get('href', '')
    if (children[0].find_all(recursive=False) != [image]
            or wrapper.find_all(recursive=False) != [caption, zoom]
            or zoom.find_all(recursive=False) != [label] or caption.find_all() or label.find_all()
            or caption.get_text(strip=True) or label.get_text(strip=True) != '圖片放大'
            or node.get_text(' ', strip=True) != '圖片放大'
            or image.get('alt') != '' or image.get('src') != target
            or zoom.get('href') != target or not target.startswith('https://iwealth.bwnet.com.tw/AC_Gallery/')):
        raise NewsError('article_identity_not_verified')
    for child in [node, *node.find_all()]:
        _own_visibility(child)


def _own_visibility(node):
    style = re.sub(r'\s+', '', str(node.get('style', ''))).lower()
    if (node.has_attr('hidden') or str(node.get('aria-hidden', '')).lower() == 'true'
            or 'display:none' in style or 'visibility:hidden' in style):
        raise NewsError('article_identity_not_verified')


def _written_body(body):
    if not body.find('p', recursive=False):
        raise NewsError('body_not_found')
    stack, ad_shells, media_shells = [body], 0, 0
    while stack:
        node = stack.pop()
        _own_visibility(node)
        if node.name in {'adcover', 'ins'}:
            if (node.parent is not body or node.contents
                    or node.name == 'adcover' and node.attrs
                    or node.name == 'ins' and (set(node.attrs) != {'pid'}
                        or not re.fullmatch('[A-F0-9]{8}(?:-[A-F0-9]{4}){3}-[A-F0-9]{12}', node['pid']))):
                raise NewsError('article_identity_not_verified')
            ad_shells += 1
            continue
        if 'article_kv' in node.get('class', []):
            if node.parent is not body:
                raise NewsError('article_identity_not_verified')
            _media_only(node)
            media_shells += 1
            continue
        if node.name not in _WRITTEN_TAGS:
            raise NewsError('article_identity_not_verified')
        stack.extend(reversed(node.find_all(recursive=False)))
    if re.search(r'本文.{0,40}(?:授權轉載|轉載自)|原文(?:刊載|出處)|資料來源[：:]', body.get_text(' ', strip=True)):
        raise NewsError('article_rights_or_scope_excluded')
    return ad_shells, media_shells


def businessweekly_wealth_public_written(soup, *, page_url):
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'wealth.businessweekly.com.tw'
            or not re.fullmatch(WEALTH_PATH_PATTERN, parts.path) or parts.fragment
            or not re.fullmatch(r'id=ARTL[0-9]{9}', parts.query)):
        raise NewsError('article_identity_not_verified')
    og_url = _one(soup, 'head > meta[property="og:url"]').get('content')
    if (_one(soup, 'head > link[rel=canonical]').get('href') != page_url
            or _hidden_declaration(soup, 'wealth_ArticleURL') != page_url
            or og_url not in {page_url, 'http:'+page_url[6:]}):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, '#main_cnt')
    _visible(scope)
    title = _one(scope, ':scope > .titxshr > h1')
    _visible(title)
    headline = title.get_text(' ', strip=True)
    if not headline or _one(soup, 'head > meta[property="og:title"]').get('content') != headline:
        raise NewsError('article_identity_not_verified')
    credit = _one(scope, WEALTH_AUTHOR_SELECTOR)
    _visible(credit)
    parent = credit.parent
    byline = credit.get_text(' ', strip=True)
    declared_writer = _hidden_declaration(soup, 'wealth_ArticleWriter')
    if (not declared_writer or declared_writer.strip() != declared_writer
            or byline != '撰文者：'+declared_writer or parent.find_all(recursive=False) != [credit]
            or parent.get_text(' ', strip=True) != byline or parent.has_attr('href') or credit.find_all()):
        raise NewsError('article_attribution_not_verified')
    dates = _one(scope, ':scope > .articlefont.smart > .article_date')
    spans = dates.select(':scope > span')
    if (len(spans) != 2 or spans[0].get('class') or spans[1].get('class') != ['counts']
            or dates.find_all(recursive=False) != [parent, *spans]):
        raise NewsError('invalid_bulk_publication_clock')
    _visible(spans[0])
    day = spans[0].get_text(strip=True)
    if day != _hidden_declaration(soup, 'wealth_ArticlePostDate') or not re.fullmatch(r'[0-9]{4}[.][0-9]{2}[.][0-9]{2}', day):
        raise NewsError('invalid_bulk_publication_clock')
    try:
        datetime.strptime(day, '%Y.%m.%d')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    single_page_link = _single_page(scope, page_url)
    body = _one(scope, WEALTH_BODY_SELECTOR)
    _visible(body)
    ads, media = _written_body(body)
    attribution = {'contract_version': 1, 'source_visible_credit': byline,
        'source_credit_role': 'visible_writer_matches_hidden_current_article_declaration',
        'source_hidden_writer_raw': declared_writer, 'original_ownership_verified': False,
        'human_reporter_identity_verified': False, 'third_party_source_credit_admitted': False}
    notices = {'contract_version': 1, 'source_OG_url_raw': og_url,
        'OG_HTTP_declaration_not_requested_or_used_as_alias': og_url.startswith('http:'),
        'visible_publication_day_raw': day, 'source_hidden_publication_day_raw': day,
        'clock_precision': 'day', 'intraday_publication_time_known': False,
        'single_visible_page_verified': True, 'unrequested_single_page_link': single_page_link,
        'validated_empty_ad_shells_excluded': ads, 'validated_captionless_media_shells_excluded': media,
        'media_or_scripts_requested_or_executed': False, 'historical_first_release_verified': False,
        'old_metadata_used_as_publication': False}
    return {'headline': headline, 'author': [{'name': declared_writer}], 'datePublished': day,
        'published_time_basis': 'visible_current_publication_day_matches_native_hidden_day; no_intraday_or_historical_first_release_proof',
        'article_identity_basis': 'literal_HTTPS_canonical_hidden_URL_query_ID_visible_title_writer_and_single_page_v1',
        'attribution_status': 'source_visible_writer_matches_native_declaration_not_verified_ownership',
        'author_attribution_detail_json': json.dumps(attribution, ensure_ascii=False, sort_keys=True),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False, sort_keys=True),
        'body_completeness': 'whole_single_static_written_body_including_summary_headings_and_values; validated_empty_ads_and_captionless_media_excluded; no_metadata_body_fallback'}, scope
