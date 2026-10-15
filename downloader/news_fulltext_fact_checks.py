"""Verified native fact-check text, not a separate crawler or CMS account map.

TFC's current WordPress report has two Kadence rows: a question/short verdict,
then a metadata/sidebar column and the actual report. Its schema author is a
CMS account, whereas the native report names its reporter and editor. Keep
those observations separate and reject an unreviewed representation.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from urllib.parse import urlsplit

from downloader.news_fulltext_cnews_cmmedia import _page_identity
from downloader.news_fulltext_policy import NewsError, _objects


TFC_BODY_SELECTOR = (
    '.entry-content.single-content > .wp-block-kadence-rowlayout:nth-of-type(2) '
    '> .kt-row-column-wrap > .wp-block-kadence-column:nth-child(2) > .kt-inside-inner-col'
)
_PATH = r'/fact-check-reports/[A-Za-z0-9_-]+/'
_NAME = r'[\u3400-\u9fff·]{2,20}'


def _one(root, selector, error='article_identity_not_verified'):
    nodes = root.select(selector)
    if len(nodes) != 1:
        raise NewsError(error)
    return nodes[0]


def _visible_native(node, article, *, error='article_identity_not_verified'):
    # Responsive class names describe alternate viewport views, not a grant
    # to read permanently hidden text. No stylesheet or media is fetched.
    nodes = [node, *node.find_all()]
    parent = node.parent
    while parent is not None:
        nodes.append(parent)
        if parent is article:
            break
        parent = parent.parent
    for value in nodes:
        if ('hidden' in value.attrs or 'inert' in value.attrs
                or str(value.get('aria-hidden', '')).strip().lower() == 'true'
                or re.search(r'(?:display\s*:\s*none|visibility\s*:\s*(?:hidden|collapse))\b',
                             str(value.get('style', '')), re.I)):
            raise NewsError(error)


def _schemas(soup, page_url):
    matches = []
    organizations = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            objects = _objects(json.loads(script.get_text()))
            for value in objects:
                kinds = value.get('@type', [])
                kinds = [kinds] if isinstance(kinds, str) else kinds
                if not isinstance(kinds, list):
                    continue
                if 'Organization' in kinds:
                    organizations.append(value)
                if 'Article' in kinds and 'Report' in kinds:
                    if (value.get('@id') != page_url + '#article'
                            or value.get('mainEntityOfPage') != {'@id': page_url}
                            or value.get('isPartOf') != {'@id': page_url}):
                        raise NewsError('article_identity_not_verified')
                    matches.append(value)
        except (json.JSONDecodeError, TypeError):
            continue
    root_url = 'https://tfc-taiwan.org.tw/'
    if (len(matches) != 1 or len(organizations) != 1
            or organizations[0].get('@id') != root_url + '#organization'
            or organizations[0].get('name') != '財團法人新北市台灣事實查核教育基金會'
            or organizations[0].get('alternateName') != '台灣事實查核中心'
            or organizations[0].get('url') != root_url
            or matches[0].get('publisher') != {'@id': root_url + '#organization'}):
        raise NewsError('article_identity_not_verified')
    if str(matches[0].get('isAccessibleForFree', 'true')).lower() == 'false':
        raise NewsError('paywall_or_login')
    return matches[0]


def _clock(raw, visible_date):
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except (TypeError, AttributeError, ValueError):
        raise NewsError('invalid_bulk_publication_clock') from None
    if (parsed.tzinfo is None or parsed.utcoffset() is None
            or parsed.strftime('%Y-%m-%d') != visible_date):
        raise NewsError('invalid_bulk_publication_clock')
    return parsed


def tfc_visible_fact_check(soup, *, page_url):
    """Keep only this report, with real bylines and source-declared clocks.

    Known CMS wordCount=16 is not a body-length/coverage assertion. Neither
    that field nor a brief verdict can replace the verified report column.
    No URL guessing, network access, caption/OCR extraction or timezone fill.
    """
    parts = urlsplit(page_url or '')
    if (parts.scheme != 'https' or parts.netloc != 'tfc-taiwan.org.tw'
            or parts.query or parts.fragment or not re.fullmatch(_PATH, parts.path)):
        raise NewsError('article_identity_not_verified')
    _page_identity(soup, page_url, 'tfc-taiwan.org.tw', _PATH)
    schema = _schemas(soup, page_url)
    headline = schema.get('headline')
    og_title = _one(soup, 'meta[property="og:title"][content]')
    if (not isinstance(headline, str) or not headline.strip()
            or og_title.get('content') != headline + ' - 台灣事實查核中心'):
        raise NewsError('article_identity_not_verified')
    article = _one(soup, 'article.entry.single-entry.type-fact-check-reports[id]')
    post = re.fullmatch(r'post-([0-9]+)', article.get('id', ''))
    if not post or 'post-' + post[1] not in article.get('class', []):
        raise NewsError('article_identity_not_verified')
    content = _one(article, ':scope > .entry-content-wrap > .entry-content.single-content')
    rows = content.select(':scope > .wp-block-kadence-rowlayout')
    if len(rows) != 2:
        raise NewsError('body_not_found')
    columns = rows[1].select(':scope > .kt-row-column-wrap > .wp-block-kadence-column')
    if len(columns) != 3 or 'kb-section-is-sticky' not in columns[0].get('class', []):
        raise NewsError('body_not_found')
    body = _one(columns[1], ':scope > .kt-inside-inner-col', 'body_not_found')
    if _one(article, TFC_BODY_SELECTOR, 'body_not_found') is not body:
        raise NewsError('body_not_found')
    _visible_native(body, article, error='body_not_found')
    blocks = body.find_all(recursive=False)
    if (not blocks or blocks[0].name != 'p'
            or [node.get_text(' ', strip=True) for node in blocks if node.name == 'h2'] != ['背景', '查核']
            or any(node.name not in {'p', 'h2', 'h3'} and not (
                node.name == 'div' and 'wp-block-kadence-image' in node.get('class', [])) for node in blocks)):
        raise NewsError('body_not_found')
    # Both responsive metadata views must agree. Never borrow a footer/related
    # author, use only an editor as reporter or map a login name to a person.
    metadata_nodes = columns[0].select('p')
    for node in metadata_nodes:
        _visible_native(node, article, error='article_attribution_not_verified')
    metadata = [node.get_text(' ', strip=True) for node in metadata_nodes]
    reporter_rows = [re.fullmatch(r'查核記者：\s*(' + _NAME + ')', text) for text in metadata]
    editor_rows = [re.fullmatch(r'責任編輯：\s*(' + _NAME + ')', text) for text in metadata]
    reporter_rows = [match[1] for match in reporter_rows if match]
    editor_rows = [match[1] for match in editor_rows if match]
    mobile = [re.fullmatch(r'記者：\s*(' + _NAME + r')、\s*責任編輯：\s*(' + _NAME + ')', text)
              for text in metadata]
    mobile = [match.groups() for match in mobile if match]
    if (len(reporter_rows) != 1 or len(editor_rows) != 1
            or mobile != [(reporter_rows[0], editor_rows[0])]):
        raise NewsError('article_attribution_not_verified')
    published = [text.removeprefix('發佈：').strip() for text in metadata if text.startswith('發佈：')]
    modified = [text.removeprefix('更新：').strip() for text in metadata if text.startswith('更新：')]
    numbers = [text.removeprefix('報告編號：').strip() for text in metadata if text.startswith('報告編號：')]
    if (len(published) != 2 or len(set(published)) != 1 or len(modified) != 1
            or any(not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date) for date in published + modified)):
        raise NewsError('invalid_bulk_publication_clock')
    if len(numbers) != 2 or len(set(numbers)) != 1 or not re.fullmatch(r'[0-9]+', numbers[0]):
        raise NewsError('article_identity_not_verified')
    first = _clock(schema.get('datePublished'), published[0])
    revision = _clock(schema.get('dateModified'), modified[0])
    if revision < first:
        raise NewsError('invalid_bulk_publication_clock')
    og_modified = _one(soup, 'meta[property="article:modified_time"][content]', 'invalid_bulk_publication_clock')
    if og_modified.get('content') != schema['dateModified']:
        raise NewsError('invalid_bulk_publication_clock')
    return {
        'headline': headline,
        'author': [{'name': reporter_rows[0]}],
        'datePublished': schema['datePublished'], 'dateModified': schema['dateModified'],
        'published_time_basis': 'source_schema_aware_clock_matched_native_visible_day; no_timezone_or_precision_inferred',
        'article_identity_basis': 'tfc_same_requested_canonical_og_schema_report_v1',
        'body_completeness': 'verified_current_fact_check_report_text; metadata_ui_and_third_party_media_excluded_v1',
        'author_attribution_detail_json': json.dumps({
            'reporter_raw': reporter_rows[0], 'responsible_editor_raw': editor_rows[0],
            'responsive_credit_rows_raw': [text for text in metadata if '記者：' in text or '責任編輯：' in text],
            'schema_cms_author_raw': schema.get('author'), 'report_number_raw': numbers[0],
            'visible_publication_date_raw': published[0], 'visible_revision_date_raw': modified[0],
            'byline_basis': 'native_named_reporter; responsible_editor_and_cms_account_are_distinct',
        }, ensure_ascii=False, sort_keys=True),
    }, article
