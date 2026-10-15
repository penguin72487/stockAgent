"""Native ETtoday original reports; shared HTTP, scheduling and receipts.

Bind the literal response to its own canonical, headline, visible reporter,
pubdate and whole story. Publisher metadata is not a person's byline. A native
bad finance time attribute is retained, not repaired or used as a timestamp.
"""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

from bs4 import Tag

from downloader.news_fulltext_policy import NewsError, _objects, normalize_url
from downloader.news_fulltext_public_originals import _one, _visible


ETTODAY_BODY_SELECTOR = ':scope > .story[itemprop="articleBody"]'
ETTODAY_PATH_PATTERN = r'^(?:/news/[1-9][0-9]*|/news/[0-9]{8}/[1-9][0-9]*[.]htm)$'
ETTODAY_HOSTS = ('finance.ettoday.net', 'www.ettoday.net')
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00')
_REPORTER = re.compile(r'記者([\u3400-\u9fff]{2,8}(?:、[\u3400-\u9fff]{2,8}){0,3})／[\u3400-\u9fff]{1,12}報導')
_CREDIT_PREFIX = re.compile(r'^(?:記者|編輯|編譯|實習記者|財經中心|生活中心|社會中心|'
    r'政治中心|國際中心|ETtoday|ETTODAY|中央社|路透社|本文來源|來源[：:]|轉載|文[／/])')
_AD_TEXT = '[廣告]請繼續往下閱讀...'


def ettoday_request_identity(url, *, requested=False):
    """No host/query replacement, fabricated ID or URL-derived release date."""
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, TypeError, ValueError):
        return None
    pattern = (r'/news/([1-9][0-9]*)' if parts.netloc == ETTODAY_HOSTS[0] else
        r'/news/[0-9]{8}/([1-9][0-9]*)[.]htm' if parts.netloc == ETTODAY_HOSTS[1] else None)
    if (parts.scheme not in ({'http', 'https'} if requested else {'https'})
            or parts.query or not pattern or not re.fullmatch(pattern, parts.path)):
        return None
    return parts.netloc, parts.path


def _key(value):
    return re.sub(r'\s+', '', value) if isinstance(value, str) else ''


def _visible_text(node):
    _visible(node)
    for value in node.find_all(string=True):
        if str(value).strip() and value.parent.name not in {'script', 'style'}:
            _visible(value.parent)


def _clocks(soup, scope, host):
    publication = _one(soup, 'head > meta[name="pubdate"][content]', 'invalid_bulk_publication_clock')
    # The actual www page repeats itemprop=dateCreated/datePublished on this
    # same pubdate meta. HTML engines select different aliases; the unique
    # native name/content and independently matching release minute control.
    if publication.get('itemprop') not in {'dateCreated', 'datePublished'}:
        raise NewsError('invalid_bulk_publication_clock')
    published = publication.get('content')
    modified = _one(soup, 'head > meta[name="lastmod"][itemprop="dateModified"][content]',
        'invalid_bulk_publication_clock').get('content')
    try:
        if not isinstance(published, str) or not isinstance(modified, str) or not (
                _CLOCK.fullmatch(published) and _CLOCK.fullmatch(modified)):
            raise ValueError('source precision or offset not verified')
        pub, mod = datetime.fromisoformat(published), datetime.fromisoformat(modified)
        if mod < pub:
            raise ValueError('revision precedes source release')
        if host == ETTODAY_HOSTS[0]:
            if 'c1' not in scope.parent.get('class', []):
                raise ValueError('current release scope not verified')
            label = _one(scope.parent, ':scope > .breadcrumb_box > time.date[itemprop="datePublished"]',
                'invalid_bulk_publication_clock')
            visible = datetime.strptime(label.get_text(' ', strip=True), '%Y-%m-%d %H:%M')
        else:
            if scope.parent.name != 'article' or 'part_area_1' not in scope.parent.parent.get('class', []):
                raise ValueError('current release scope not verified')
            label = _one(scope.parent.parent, ':scope > .part_breadcrumb_2 > time.date[itemprop="datePublished"]',
                'invalid_bulk_publication_clock')
            visible = datetime.strptime(label.get_text(' ', strip=True), '%Y年%m月%d日 %H:%M')
        _visible_text(label)
        if visible != pub.replace(tzinfo=None, second=0):
            raise ValueError('native minute conflicts with pubdate')
        native_datetime = label.get('datetime')
        if native_datetime != published and not (host == ETTODAY_HOSTS[0] and native_datetime == '31'):
            raise ValueError('unreviewed native datetime anomaly')
        extra = soup.select('head > meta[property="article:published_time"][content]')
        if extra and (len(extra) != 1 or extra[0].get('content') != published):
            raise ValueError('competing release declarations')
    except (ValueError, TypeError, AttributeError):
        raise NewsError('invalid_bulk_publication_clock') from None
    notices = [{'kind': 'native_pubdate', 'text': published},
        {'kind': 'native_lastmod', 'text': modified},
        {'kind': 'native_visible_release_minute', 'text': label.get_text(' ', strip=True)}]
    if native_datetime == '31':
        notices.append({'kind': 'source_invalid_datetime_attribute_preserved', 'text': '31'})
    return published, modified, notices


def _native_json(text):
    """Two observed source syntax defects, without changing a source value.

    Literal CR/LF inside a description and one exact source author comment are
    not valid JSON. A bounded lexer distinguishes strings from comments; never
    delete arbitrary comments/bytes, replace quotes or use a forgiving JS eval.
    Original response bytes and syntax anomalies remain in the receipt chain.
    """
    try:
        return json.loads(text), []
    except json.JSONDecodeError:
        pass
    output, anomalies, quoted, escaped, field, start, index = [], [], False, False, '', 0, 0
    while index < len(text):
        char = text[index]
        if quoted:
            if escaped:
                output.append(char)
                escaped = False
            elif char == '\\':
                output.append(char)
                escaped = True
            elif char == '"':
                output.append(char)
                quoted = False
                cursor = index + 1
                while cursor < len(text) and text[cursor].isspace():
                    cursor += 1
                if cursor < len(text) and text[cursor] == ':':
                    field = json.loads(text[start:index + 1])
            elif ord(char) < 32:
                if field != 'description' or char not in '\r\n':
                    raise ValueError('unreviewed raw string control')
                output.append('\\r' if char == '\r' else '\\n')
                anomalies.append('source_unescaped_description_linebreak')
            else:
                output.append(char)
        elif char == '"':
            output.append(char)
            quoted, start = True, index
        elif char == '/' and text[index:index + 2] == '//':
            end = text.find('\n', index)
            if end < 0:
                raise ValueError('unreviewed JSON comment')
            if (field != 'name' or text[index:end].strip() != '//如果有筆名，才會有url'
                    or 'source_non_json_author_comment' in anomalies):
                raise ValueError('unreviewed JSON comment')
            anomalies.append('source_non_json_author_comment')
            index = end
            continue
        else:
            output.append(char)
        index += 1
    if quoted or not anomalies:
        raise ValueError('source JSON not verified')
    return json.loads(''.join(output)), list(dict.fromkeys(anomalies))


def _schemas(soup, original, title, published, modified, names):
    declarations = []
    notices = []
    scripts = soup.select('script[type="application/ld+json"]')
    try:
        if len(scripts) > 16:
            raise ValueError('bounded article schema required')
        for script in scripts:
            if len(script.get_text()) > 262144:
                raise ValueError('bounded schema required')
            data, anomalies = _native_json(script.get_text())
            declarations.extend(obj for obj in _objects(data)
                if obj.get('@type') in ('NewsArticle', 'Article'))
            # HTML parsers normalize line endings differently. The raw
            # response hash lives in the shared capture receipt, not a hash
            # falsely labelled "original" after DOM normalization.
            notices.extend({'kind': kind, 'raw_evidence': 'shared_original_response_receipt'} for kind in anomalies)
        if len(declarations) > 1:
            raise ValueError('ambiguous current article')
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    for article in declarations:
        if (article.get('url') != original or article.get('mainEntityOfPage') != original
                or _key(article.get('headline')) != _key(title) or article.get('articleBody')):
            raise NewsError('article_identity_not_verified')
        free = article.get('isAccessibleForFree')
        if free is not None and free is not True:
            raise NewsError('paywall_or_login')
        if article.get('datePublished') != published or article.get('dateModified') != modified:
            raise NewsError('invalid_bulk_publication_clock')
        author, publisher = article.get('author'), article.get('publisher')
        organization = author == {'@type': 'Organization', 'name': 'ETtoday新聞雲', 'url': 'https://www.ettoday.net/'}
        authors = author if isinstance(author, list) else [author]
        reporters = (len(authors) == len(names)
            and all(isinstance(a, dict) and a.get('@type') == 'Person' for a in authors)
            and [a.get('name') for a in authors] == names)
        last_of_two = (len(names) == 2 and len(authors) == 1 and isinstance(authors[0], dict)
            and authors[0].get('@type') == 'Person' and authors[0].get('name') == names[-1])
        if (not (organization or reporters or last_of_two) or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
                or _key(publisher.get('name')) != 'ETtoday新聞雲'):
            raise NewsError('article_rights_or_scope_excluded')
        if organization:
            notices.append({'kind': 'schema_organization_author_not_reporter', 'text': 'ETtoday新聞雲'})
        elif last_of_two:
            notices.append({'kind': 'schema_declares_last_of_two_native_reporters', 'text': names[-1]})
    return notices


def _original_body(body):
    """Whole observed story, minus verified UI, credits and image captions.

    Caption/attribution are retained separately. Do not use image alt text,
    descriptions, neighboring stories, hidden text or an arbitrary prose cutoff.
    """
    cleaned = deepcopy(body)
    captions, credit, names = [], None, []
    paragraphs = cleaned.find_all('p', recursive=False)
    for index, node in enumerate(paragraphs):
        text = node.get_text(' ', strip=True)
        match = _REPORTER.fullmatch(text)
        if match:
            if credit is not None or index > 5:
                raise NewsError('article_attribution_not_verified')
            _visible_text(node)
            credit, names = text, match[1].split('、')
            if len(set(names)) != len(names):
                raise NewsError('article_attribution_not_verified')
            node.decompose()
        elif credit is None and _CREDIT_PREFIX.match(text):
            raise NewsError('article_rights_or_scope_excluded')
        elif text.startswith('▲') and index and paragraphs[index - 1].find('img'):
            _visible_text(node)
            captions.append(text)
            node.decompose()
        elif credit is None and text:
            # Only the actual image prelude may precede the newsroom credit.
            raise NewsError('article_attribution_not_verified')
    if credit is None:
        raise NewsError('article_attribution_not_verified')
    for node in list(cleaned.select(':scope > div.ad_in_news')):
        labels = node.select(':scope > .ad_readmore')
        if (len(labels) != 1 or labels[0].get_text(' ', strip=True) != _AD_TEXT
                or node.get_text(' ', strip=True) != _AD_TEXT
                or any(child.name not in {'div', 'script'} for child in node.find_all(recursive=False))):
            raise NewsError('article_identity_not_verified')
        node.decompose()
    if cleaned.select('script,style,article,main,.story,[itemprop="articleBody"]'):
        raise NewsError('article_identity_not_verified')
    _visible_text(cleaned)
    return cleaned, names, credit, captions


def ettoday_visible_article(soup, *, page_url):
    identity = ettoday_request_identity(page_url)
    if identity is None:
        raise NewsError('article_identity_not_verified')
    host, path = identity
    original = 'https://' + host + path
    canonical = _one(soup, 'head > link[rel~="canonical"][itemprop="mainEntityOfPage"][href]')
    ogurl = _one(soup, 'head > meta[property="og:url"][content]')
    ogtitle = _one(soup, 'head > meta[property="og:title"][content]')
    scope = _one(soup, '.subject_article')
    heading = _one(scope, ':scope > header > h1.title[itemprop="headline"]')
    body = _one(scope, ETTODAY_BODY_SELECTOR, 'body_not_found')
    section = _one(soup, 'head > meta[name="subsection"][itemprop="articleSection"][content]').get('content')
    title = heading.get_text(' ', strip=True)
    suffix = ' | ETtoday財經雲 | ETtoday新聞雲' if host == ETTODAY_HOSTS[0] else f' | ETtoday{section}新聞 | ETtoday新聞雲'
    if (canonical.get('href') != original or ogurl.get('content') != original or not title
            or not isinstance(section, str) or not re.fullmatch(r'[\u3400-\u9fff]{1,16}', section)
            or _key(ogtitle.get('content')) != _key(title + suffix)
            or len(soup.select('.story[itemprop="articleBody"]')) != 1
            or soup.html.get('itemtype') not in {'http://schema.org/NewsArticle', 'https://schema.org/NewsArticle'}):
        raise NewsError('article_identity_not_verified')
    _visible_text(heading)
    _visible(body)
    cleaned, names, credit, captions = _original_body(body)
    published, modified, notices = _clocks(soup, scope, host)
    notices += _schemas(soup, original, title, published, modified, names)
    notices.append({'kind': 'native_reporter_credit', 'text': credit})
    for node in scope.select(':scope > p.note'):
        _visible_text(node)
        notice = node.get_text(' ', strip=True)
        if re.search(r'禁止(?:下載|抓取)|不得(?:下載|抓取)', notice):
            raise NewsError('article_rights_or_scope_excluded')
        notices.append({'kind': 'native_article_rights_notice', 'text': notice})
    article = {'headline': title, 'author': [{'@type': 'Person', 'name': name} for name in names],
        'datePublished': published, 'dateModified': modified,
        'published_time_basis': 'native_pubdate_offset_seconds_verified_against_visible_release_minute; not_capture_time',
        'article_identity_basis': 'literal_host_path_canonical_og_microdata_headline_native_reporter_and_release',
        'body_completeness': 'verified_whole_native_story; source_reporter_captions_and_rights_notices_retained_separately',
        'author_attribution_detail_json': json.dumps([{'name': name, 'role': 'reporter',
            'native_credit': credit, 'basis': 'visible_current_story_reporter_signature'} for name in names], ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False)}
    # Do not copy the whole subject and the story a second time. All metadata
    # was validated against the original document above; generic extraction
    # needs only its verified heading and one unchanged-order cleaned story.
    wrapper = Tag(name='div')
    wrapper.append(deepcopy(heading))
    wrapper.append(cleaned)
    return article, wrapper


def ettoday_validation_metadata(soup, *, page_url):
    """Bounded failure anatomy on the already fetched response, not an article.

    Reuse the owner's persisted daily diagnostic reservation. Export numeric
    clock declarations and credit/schema shapes only, never prose, names,
    scripts, raw HTML, credentials or permission to retain a failed article.
    """
    identity = ettoday_request_identity(page_url)
    facts = {'contract_version': 1, 'literal_route_verified': identity is not None,
        'body_text_retained': False, 'raw_scope_or_article_acceptance_granted': False}
    if identity is None:
        return facts
    compact_hash = lambda value: hashlib.sha256(_key(value).encode()).hexdigest() if isinstance(value, str) else None
    safe_clock = lambda value: value if isinstance(value, str) and _CLOCK.fullmatch(value) else None
    stories = soup.select('.subject_article > .story[itemprop="articleBody"]')
    credits = []
    for story in stories[:4]:
        for index, node in enumerate(story.find_all('p', recursive=False)[:8]):
            text = node.get_text(' ', strip=True)
            match = _REPORTER.fullmatch(text)
            if match or _CREDIT_PREFIX.match(text):
                credits.append({'paragraph_index': index, 'native_reporter_signature_shape': bool(match),
                    'reporter_count': len(match[1].split('、')) if match else None,
                    'credit_compact_sha256': compact_hash(text)})
    releases = soup.select('time[itemprop="datePublished"]')
    clocks = []
    for node in releases[:4]:
        text = node.get_text(' ', strip=True)
        minute = bool(re.fullmatch(r'(?:[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}|'
            r'[0-9]{4}年[0-9]{2}月[0-9]{2}日 [0-9]{2}:[0-9]{2})', text))
        clocks.append({'visible_minute': text if minute else None,
            'native_datetime': safe_clock(node.get('datetime')),
            'observed_finance_datetime_31': node.get('datetime') == '31'})
    declarations = []
    syntax, schema_error = [], False
    scripts = soup.select('script[type="application/ld+json"]')
    if len(scripts) > 16:
        schema_error = True
    else:
        try:
            for script in scripts:
                if len(script.get_text()) > 262144:
                    raise ValueError('bounded schema required')
                value, anomalies = _native_json(script.get_text())
                syntax.extend(anomalies)
                declarations.extend(obj for obj in _objects(value) if obj.get('@type') in {'NewsArticle', 'Article'})
                if len(declarations) > 8:
                    raise ValueError('bounded declarations required')
        except (ValueError, TypeError, RecursionError):
            schema_error = True
    facts.update(host=identity[0], native_story_count=len(stories), native_credit_shapes=credits,
        native_release_node_count=len(releases), native_clock_labels=clocks,
        declared_publication=[safe_clock(n.get('content')) for n in soup.select('head > meta[name="pubdate"]')[:4]],
        declared_modification=[safe_clock(n.get('content')) for n in soup.select('head > meta[name="lastmod"]')[:4]],
        schema_error=schema_error, article_schema_count=len(declarations),
        observed_schema_syntax_anomalies=sorted(set(syntax)),
        metadata_truncated=len(stories) > 4 or len(releases) > 4 or len(declarations) > 8,
        declared_author_types=[[a.get('@type') if isinstance(a, dict) and a.get('@type') in {'Person', 'Organization'}
            else 'unknown' for a in (obj.get('author') if isinstance(obj.get('author'), list)
                else [obj.get('author')])[:8]] for obj in declarations[:8]])
    return facts
