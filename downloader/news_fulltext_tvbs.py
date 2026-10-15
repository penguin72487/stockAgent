"""TVBS native reports: three layouts, shared HTTP/queue/receipts unchanged.

Literal locally known URLs only. A matching numeric ID is admission, not body
proof. Canonical/OG/schema, current headline, native reporter/editor and source
clocks must bind the actual response; never use descriptions, React scripts or
a neighboring story. No requests or source-policy decisions live in this module.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from urllib.parse import parse_qsl, urljoin, urlsplit

from bs4 import BeautifulSoup

from downloader.news_fulltext_policy import NewsError, _objects, normalize_url
from downloader.news_fulltext_public_originals import _one, _visible


TVBS_BODY_SELECTOR = ':scope > .tvbs-native-body'
TVBS_PATH_PATTERN = '^(?:/[a-z][a-z-]*/[1-9][0-9]*|/exhibition/ai-trends-2026/article_(?:1|3)-3[.]html)$'
TVBS_HOSTS = ('news.tvbs.com.tw', 'cars.tvbs.com.tw', 'supertaste.tvbs.com.tw')
_TW = timezone(timedelta(hours=8))
_EXTERNAL_CREDIT = re.compile(r'^(?:[（(]中央社|[（(]?(?:中央社(?:記者|／|/)|CNEWS|匯流新聞網|CTWANT|周刊王|轉載[：:]))')
_EXTERNAL_AUTHORS = {'中央社', '中央通訊社', 'CNA', 'CNEWS', '匯流新聞網', 'CTWANT', '周刊王'}
TVBS_EXHIBITION_LAYOUTS = {
    '/exhibition/ai-trends-2026/article_1-3.html': ('menu1-fq', 'article01-8w', 'image3-8y',
        'content01-8x', 'content01-8z', 'slider2-93', 'footer7-fw'),
    '/exhibition/ai-trends-2026/article_3-3.html': ('menu1-fq', 'article01-ap', 'image3-aq',
        'content01-ar', 'image3-au', 'content01-as', 'content01-at', 'content01-bm', 'image3-bk',
        'content01-av', 'image3-bl', 'content01-bn', 'slider2-aw', 'footer7-fw'),
}


def tvbs_request_identity(url, *, requested=False):
    """Do not remove a query or change a host to obtain another response.

    Old local links include HTTP and the publisher's literal `from` UI query.
    Only transport may follow redirects. An actual query-bearing report still
    needs the queryless canonical, OG and whole native proof below. Unknown
    parameters are not presumed tracking or ignored for identity purposes.
    """
    try:
        parts = urlsplit(normalize_url(url))
        query = parse_qsl(parts.query, keep_blank_values=True)
    except (NewsError, TypeError, ValueError):
        return None
    if (parts.scheme not in ({'http', 'https'} if requested else {'https'})
            or parts.netloc not in TVBS_HOSTS
            or not re.fullmatch(TVBS_PATH_PATTERN, parts.path)
            or parts.path in TVBS_EXHIBITION_LAYOUTS and parts.netloc != TVBS_HOSTS[0]
            or parts.query and (len(query) != 1 or query[0][0] != 'from' or not query[0][1])):
        return None
    return parts.netloc, parts.path


def _key(value):
    return re.sub(r'\s+', '', value) if isinstance(value, str) else ''


def _visible_tree(node):
    for child in (node, *node.find_all(True)):
        _visible(child)


def _visible_text_tree(node):
    """Native credit text must be visible; empty decorative SVGs are not it."""
    _visible(node)
    for text in node.find_all(string=True):
        if str(text).strip():
            _visible(text.parent)


def _schema(soup, page_url):
    identity = tvbs_request_identity(page_url)
    if identity is None:
        raise NewsError('article_identity_not_verified')
    host, path = identity
    original = 'https://' + host + path
    canonical = _one(soup, 'link[rel~="canonical"][href]')
    ogurl = _one(soup, 'meta[property="og:url"][content]')
    if canonical.get('href') != original or ogurl.get('content') != original:
        raise NewsError('article_identity_not_verified')
    scripts = soup.select('script[type="application/ld+json"]')
    declared, objects = [], []
    try:
        if len(scripts) > 32:
            raise ValueError('unbounded schema')
        for script in scripts:
            text = script.get_text()
            if len(text) > 262144:
                raise ValueError('unbounded schema')
            objects.extend(_objects(json.loads(text)))
        declared = [obj for obj in objects if obj.get('@type') in ('Article', 'NewsArticle')]
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    if len(declared) != 1:
        raise NewsError('article_identity_not_verified')
    article = declared[0]
    expected_id = original if host == 'supertaste.tvbs.com.tw' else original + '#article'
    if (article.get('@id') != expected_id
            or article.get('mainEntityOfPage') != {'@type': 'WebPage', '@id': original}
            or article.get('url', original) != original or article.get('articleBody')):
        raise NewsError('article_identity_not_verified')
    if article.get('isAccessibleForFree') is not None and str(article['isAccessibleForFree']).lower() not in {
            'true', 'http://schema.org/true', 'https://schema.org/true'}:
        raise NewsError('paywall_or_login')
    publisher = article.get('publisher')
    expected_name = dict(zip(TVBS_HOSTS, ('TVBS新聞網', '地球黃金線', '食尚玩家')))[host]
    if host == 'news.tvbs.com.tw':
        organizations = [obj for obj in objects if obj.get('@type') == 'NewsMediaOrganization'
            and obj.get('@id') == 'https://' + host + '/#organization']
        if (publisher != {'@id': 'https://' + host + '/#organization'} or len(organizations) != 1
                or organizations[0].get('name') != expected_name
                or organizations[0].get('url') != 'https://' + host + '/'):
            raise NewsError('article_identity_not_verified')
    elif (not isinstance(publisher, dict) or publisher.get('name') != expected_name
            or publisher.get('@type') not in {'Organization', 'NewsMediaOrganization'}):
        raise NewsError('article_identity_not_verified')
    return article, host, original


def _clocks(soup, article, *, adapter_contract_version=1, host=None):
    clocks, notices = {}, []
    try:
        for field, meta in (('datePublished', 'article:published_time'), ('dateModified', 'article:modified_time')):
            value = article.get(field)
            if not isinstance(value, str) or not re.fullmatch(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]{1,6})?(?:Z|\+08:00)', value):
                raise ValueError('source precision/timezone unverified')
            clocks[field] = datetime.fromisoformat(value.replace('Z', '+00:00'))
            node = _one(soup, 'meta[property="' + meta + '"][content]', 'invalid_bulk_publication_clock')
            if node.get('content') != value:
                raise ValueError('competing schema/meta clocks')
            notices.append({'kind': 'native_declared_' + field, 'text': value})
        if clocks['dateModified'] < clocks['datePublished']:
            # Two actual native news pages declare a last-save timestamp
            # exactly one second before release, with the same visible minute.
            # Retain this source anomaly, not a repaired time or historical
            # availability claim. Other hosts/order anomalies remain errors.
            same_minute = clocks['dateModified'].astimezone(_TW).strftime('%Y.%m.%d %H:%M') == clocks['datePublished'].astimezone(_TW).strftime('%Y.%m.%d %H:%M')
            if not (adapter_contract_version == 2 and host == TVBS_HOSTS[0]
                    and clocks['datePublished'] - clocks['dateModified'] == timedelta(seconds=1)
                    and same_minute):
                raise ValueError('revision before publication')
    except (ValueError, TypeError):
        raise NewsError('invalid_bulk_publication_clock') from None
    return clocks, notices


def _author(article):
    authors = article.get('author')
    authors = authors if isinstance(authors, list) else [authors]
    if (len(authors) != 1 or not isinstance(authors[0], dict) or authors[0].get('@type') != 'Person'
            or not isinstance(authors[0].get('name'), str) or not authors[0]['name'].strip()):
        raise NewsError('article_attribution_not_verified')
    if authors[0]['name'].strip() in _EXTERNAL_AUTHORS:
        raise NewsError('article_rights_or_scope_excluded')
    return authors[0]


def _news(soup, article, clocks, notices, original, *, adapter_contract_version=1):
    heading = _one(soup, 'main > div > div > header > h1')
    header, scope = heading.parent, heading.parent.parent
    body = _one(scope, ':scope > main .article-editor-content', 'body_not_found')
    labels = header.select('li')
    counts = {'datePublished': 0, 'dateModified': 0, 'byline': 0}
    roles = set()
    authors = article.get('author')
    authors = authors if isinstance(authors, list) else [authors]
    if adapter_contract_version == 2:
        if len(authors) not in {1, 2}:
            raise NewsError('article_attribution_not_verified')
        # Preserve the publisher's ordered reporter + responsible-editor
        # declarations, rather than dropping the editor or inventing a person.
        authors = [_author({'author': author}) for author in authors]
        if any(author['name'].strip() == '品觀點' for author in authors):
            raise NewsError('article_rights_or_scope_excluded')
        if len({_key(author['name']) for author in authors}) != len(authors):
            raise NewsError('article_attribution_not_verified')
        if any(len(author['name']) > 80 or re.search(r'[:：｜|]', author['name']) for author in authors):
            raise NewsError('article_attribution_not_verified')
    else:
        authors = [_author(article)]
    author = authors[0]
    role_pattern = r'(記者|編輯|編譯)' if adapter_contract_version == 2 else r'(記者|編輯)'
    credit_pattern = role_pattern + '：' + re.escape(_key(author['name']))
    if len(authors) == 2:
        credit_pattern += r'｜責任編輯：' + re.escape(_key(authors[1]['name']))
    for node in labels:
        _visible_tree(node)
        text = node.get_text(' ', strip=True)
        compact = _key(text)
        credit = re.fullmatch(credit_pattern, compact)
        if credit:
            counts['byline'] += 1
            roles.add(credit[1])
            kind = {'記者': 'native_reporter_credit', '編輯': 'native_editor_credit', '編譯': 'native_translator_credit'}[credit[1]]
            if len(authors) == 2:
                kind += '_with_responsible_editor'
            notices.append({'kind': kind, 'text': text})
            continue
        match = re.fullmatch(r'(發佈時間|最後更新時間)：\s*([0-9]{4}[.][0-9]{2}[.][0-9]{2} [0-9]{2}:[0-9]{2})', text)
        if match is None:
            raise NewsError('article_rights_or_scope_excluded' if '：' in compact and '時間' not in compact
                else 'invalid_bulk_publication_clock')
        field = 'datePublished' if match[1] == '發佈時間' else 'dateModified'
        if clocks[field].astimezone(_TW).strftime('%Y.%m.%d %H:%M') != match[2]:
            raise NewsError('invalid_bulk_publication_clock')
        counts[field] += 1
        notices.append({'kind': 'native_visible_' + field + '_minute', 'text': text})
    if counts['byline'] not in {1, 2} or len(roles) != 1:
        raise NewsError('article_attribution_not_verified')
    if counts['datePublished'] not in {1, 2} or counts['dateModified'] != counts['datePublished']:
        raise NewsError('invalid_bulk_publication_clock')
    cleaned = deepcopy(body)
    for node in list(cleaned.select(':scope > aside')):
        if not node.get_text(' ', strip=True).startswith('更多新聞：'):
            raise NewsError('article_identity_not_verified')
        node.decompose()
    for node in list(cleaned.select(':scope > .inline-pc-ad-wrapper, :scope > .inline-mobile-ad-wrapper')):
        if node.get_text(' ', strip=True):
            raise NewsError('article_identity_not_verified')
        node.decompose()
    for node in list(cleaned.select(':scope > div')):
        links = node.select(':scope > a[href]')
        if (len(links) == 1 and len(node.find_all(recursive=False)) == 1
                and links[0].get('href', '').startswith('https://roundtable-tech-zh.vive.com/query?')
                and node.get_text(' ', strip=True) in {'到VIVE科技圓桌討論，聽聽專家怎麼說', '到VIVE科技圓桌討論，聽聽專家怎麼說 >'}):
            node.decompose()
    return heading, body, cleaned, authors if len(authors) == 2 else author, 'native_header_author_role_and_release_revision_minutes'


def _cars(soup, article, clocks, notices, original):
    scope = _one(soup, 'main > article')
    heading = _one(scope, ':scope > div > h1')
    header = heading.parent
    author = _author(article)
    link = _one(header, 'a[href^="/author/"]', 'article_attribution_not_verified')
    credit = link.get_text(' ', strip=True)
    role = author.get('jobTitle')
    if (role not in {'記者', '編輯'} or credit != role + ' ' + author['name']
            or urljoin(original, link.get('href')) != author.get('url')
            or not re.fullmatch(r'https://cars[.]tvbs[.]com[.]tw/author/[a-z][a-z0-9_-]*', str(author.get('url', '')))):
        raise NewsError('article_rights_or_scope_excluded')
    clock = _one(header, 'time[datetime]', 'invalid_bulk_publication_clock')
    if (clock.get('datetime') != article['datePublished']
            or clock.get_text(' ', strip=True) != clocks['datePublished'].astimezone(_TW).strftime('%Y/%m/%d')):
        raise NewsError('invalid_bulk_publication_clock')
    _visible_tree(link)
    _visible_tree(clock)
    notices += [{'kind': 'native_reporter_credit' if role == '記者' else 'native_editor_credit', 'text': credit},
        {'kind': 'native_visible_release_date', 'text': clock.get_text(' ', strip=True)}]
    body = _one(scope, ':scope > div.items-start > div.gap-article-section-gap', 'body_not_found')
    cleaned = deepcopy(body)
    # Older reports use a native label plus a linked list, not the current
    # related-story div. Remove only that exact adjacent UI shape; a prose
    # paragraph mentioning further reading or an actual data list stays intact.
    for label in list(cleaned.select(':scope > p.article-paragraph')):
        if (label.get_text(' ', strip=True) != '延伸閱讀：'
                or len(label.find_all(recursive=False)) != 1
                or label.find_all(recursive=False)[0].name != 'b'):
            continue
        links = label.find_next_sibling()
        if links is None or links.name != 'ul':
            continue
        items = links.find_all(recursive=False)
        if not 1 <= len(items) <= 64 or any(item.name != 'li' for item in items):
            continue
        valid = True
        for item in items:
            anchors = item.select('a[href]')
            target = tvbs_request_identity(anchors[0].get('href')) if len(anchors) == 1 else None
            if (target is None or target[0] != 'cars.tvbs.com.tw' or target == tvbs_request_identity(original)
                    or item.get_text(' ', strip=True) != anchors[0].get_text(' ', strip=True)):
                valid = False
                break
        if valid:
            label.decompose()
            links.decompose()
    for node in list(cleaned.select(':scope > div')):
        children = node.find_all(recursive=False)
        if children and children[0].name == 'p' and children[0].get_text(' ', strip=True) == '延伸閱讀':
            if not all(child.name == 'a' and child.get('href', '').startswith('/') for child in children[1:]):
                raise NewsError('article_identity_not_verified')
            node.decompose()
        elif children and all(child.name == 'a' and child.get('href', '').startswith('/tag/') for child in children):
            node.decompose()
        elif not node.get_text(' ', strip=True):
            node.decompose()
    return heading, body, cleaned, author, 'native_header_author_role_account_and_release_datetime_date'


def _supertaste(soup, article, clocks, notices, original):
    scope = _one(soup, 'article#article-content')
    header = _one(scope, ':scope > header')
    heading = _one(header, ':scope > h1')
    author = _author(article)
    link = _one(header, 'a[href^="/author/"]', 'article_attribution_not_verified')
    credit = link.get_text(' ', strip=True)
    if (not re.fullmatch('《食尚玩家》(?:資深)?記者 ' + re.escape(author['name']), credit)
            or urljoin(original, link.get('href')) != author.get('@id')):
        raise NewsError('article_rights_or_scope_excluded')
    native_node = _one(header, ':scope > div.mb-4', 'article_attribution_not_verified')
    native = native_node.get_text(' ', strip=True)
    if not re.search(r'｜撰文\s*' + re.escape(author['name']) + r'｜', native):
        raise NewsError('article_attribution_not_verified')
    # Validate the actual attribution, not decorative hidden SVGs in the
    # unrelated navigation/share controls of this same header.
    _visible_tree(link)
    _visible_text_tree(native_node)
    notices += [{'kind': 'native_reporter_credit', 'text': credit}, {'kind': 'native_text_and_image_credit', 'text': native}]
    marker = _one(scope, ':scope > #article-end-marker', 'body_not_found')
    if marker.get_text(' ', strip=True):
        raise NewsError('article_identity_not_verified')
    cleaned = deepcopy(scope)
    clean_marker = _one(cleaned, ':scope > #article-end-marker')
    for node in list(clean_marker.next_siblings):
        node.extract()  # Publisher end marker, not an arbitrary prose cutoff.
    clean_marker.decompose()
    _one(cleaned, ':scope > header').decompose()
    for node in list(cleaned.find_all(recursive=False)):
        text = node.get_text(' ', strip=True)
        if node.name == 'section':
            if node.get('id') == 'coupon_block' and text.startswith('本週優惠券，領起來'):
                node.decompose()
            elif text.startswith('文章目錄') and all(a.get('href', '').startswith('#') for a in node.select('a[href]')):
                node.decompose()
            elif text.startswith('達人推薦'):
                notices.append({'kind': 'native_contributor_credit', 'text': text})
                node.decompose()
        elif node.name == 'div':
            if text == '廣告 - 內文未完請往下捲動' and node.select('[id^="supertaste_pc_read_in"], [id^="supertaste_m_read_in"]'):
                node.decompose()
            elif node.select('a[href^="https://supertaste.tvbs.com.tw/infocard/"]'):
                notices.append({'kind': 'native_venue_information_card', 'text': text})
                node.decompose()
    return heading, scope, cleaned, author, 'native_header_reporter_account_and_matching_declared_release_revision_clocks'


def _exhibition(soup, *, page_url):
    """Two actually known segmented reports, not a wildcard exhibition crawler.

    These pages declare their reporter and day visibly, and list that reporter
    in the native newsroom credits. They have no Article schema/OG headline or
    intraday release clock. Verify their own contract rather than manufacturing
    the regular NewsArticle contract, an author account or a midnight timestamp.
    """
    identity = tvbs_request_identity(page_url)
    if identity is None or identity[0] != TVBS_HOSTS[0] or identity[1] not in TVBS_EXHIBITION_LAYOUTS:
        raise NewsError('article_identity_not_verified')
    path = identity[1]
    original = 'https://' + identity[0] + path
    canonical = _one(soup, 'link[rel~="canonical"][href]')
    if canonical.get('href') != original or soup.select('h1, time, meta[name="author"],'
            'meta[property="og:url"], meta[property="og:title"], meta[property^="article:"],'
            'script[type="application/ld+json"]'):
        raise NewsError('article_identity_not_verified')
    sections = soup.select('body > section')
    if tuple(node.get('id') for node in sections) != TVBS_EXHIBITION_LAYOUTS[path]:
        raise NewsError('article_identity_not_verified')
    header = _one(soup, 'section.article01')
    heading = _one(header, 'h2.mbr-section-title')
    title = heading.get_text(' ', strip=True)
    declared_title = _one(soup, 'title').get_text(' ', strip=True)
    if not title or _key(declared_title) != _key(title + '｜TVBS新聞網'):
        raise NewsError('article_identity_not_verified')
    credit = _one(header, 'p.mbr-text.display-4', 'article_attribution_not_verified')
    label = credit.get_text(' ', strip=True)
    match = re.fullmatch(r'◎ 報導／(?P<name>\S+) ◎ 發布日期／(?P<day>[0-9]{4}[.][0-9]{2}[.][0-9]{2})', label)
    if match is None or match['name'] in _EXTERNAL_AUTHORS:
        raise NewsError('article_attribution_not_verified')
    try:
        datetime.strptime(match['day'], '%Y.%m.%d')
    except ValueError:
        raise NewsError('invalid_bulk_publication_clock') from None
    footer = _one(soup, 'section.footer7')
    credits = footer.select('p')
    if len(credits) != 2:
        raise NewsError('article_attribution_not_verified')
    if (credits[0] is not _one(footer, 'p.card-text.display-4', 'article_attribution_not_verified')
            or credits[1] is not _one(footer, 'p.mbr-text.display-4', 'article_attribution_not_verified')):
        raise NewsError('article_attribution_not_verified')
    staff, copyright_label = [node.get_text(' ', strip=True) for node in credits]
    newsroom = re.search(r'網路新聞中心\s+(.+?)\s+編播中心', staff)
    if (newsroom is None or match['name'] not in [name.strip() for name in newsroom[1].split('・')]
            or copyright_label != '© TVBS Media Inc. All Rights Reserved.｜聯利媒體股份有限公司'):
        raise NewsError('article_rights_or_scope_excluded')
    for node in (heading, credit, *credits):
        _visible_tree(node)
    prose = [node for node in sections if 'content1' in node.get('class', [])]
    images = [node for node in sections if 'image3' in node.get('class', [])]
    expected_prose = [section for section in TVBS_EXHIBITION_LAYOUTS[path] if section.startswith('content01-')]
    expected_images = [section for section in TVBS_EXHIBITION_LAYOUTS[path] if section.startswith('image3-')]
    if [node.get('id') for node in prose] != expected_prose or [node.get('id') for node in images] != expected_images:
        raise NewsError('article_identity_not_verified')
    wrapper = BeautifulSoup('', 'lxml').new_tag('div')
    body = BeautifulSoup('', 'lxml').new_tag('div', attrs={'class': ['tvbs-native-body']})
    for section in prose:
        if (len(section.select('p.mbr-text.display-7')) != 1 or len(section.select('h3')) > 1
                or section.select('script, iframe, form, article, [rel~="license"]')):
            raise NewsError('article_rights_or_scope_excluded')
        _visible_tree(section)
        for paragraph in section.select('p'):
            if _EXTERNAL_CREDIT.match(paragraph.get_text(' ', strip=True)):
                raise NewsError('article_rights_or_scope_excluded')
        body.append(deepcopy(section))
    captions = []
    for section in images:
        _visible_tree(section)
        caption = _one(section, 'p.mbr-description.display-4')
        captions.append(caption.get_text(' ', strip=True))
    wrapper.append(body)
    return {'headline': title, 'author': [{'@type': 'Person', 'name': match['name']}],
        'datePublished': match['day'],
        'article_identity_basis': 'exact_known_exhibition_URL_canonical_visible_heading_native_title_segment_sequence_reporter_newsroom_and_copyright_v1',
        'author_attribution_detail_json': json.dumps({'native_author_name': match['name'], 'native_role': '報導',
            'source_author_account': None, 'author_name_invented': False}, ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'verified_article_notices_json': json.dumps([{'kind': 'native_reporter_and_release_day_credit', 'text': label},
            {'kind': 'native_newsroom_staff_credits', 'text': staff},
            {'kind': 'native_publisher_copyright', 'text': copyright_label}], ensure_ascii=False),
        'published_time_basis': 'publisher_visible_date_only; source_timezone_and_intraday_time_unknown; not_observed_historical_PIT',
        'body_completeness': 'verified_current_whole_native_segmented_report_prose; all_content_segments_and_headings_retained; image_captions_and_native_newsroom_copyright_recorded_separately; navigation_slider_related_stories_and_UI_excluded'}, wrapper


def tvbs_visible_article(soup, *, page_url, adapter_contract_version=1):
    if adapter_contract_version not in {1, 2}:
        raise ValueError('reviewed_tvbs_adapter_version_required')
    identity = tvbs_request_identity(page_url)
    if identity and identity[1] in TVBS_EXHIBITION_LAYOUTS:
        return _exhibition(soup, page_url=page_url)
    article, host, original = _schema(soup, page_url)
    clocks, notices = _clocks(soup, article, adapter_contract_version=adapter_contract_version, host=host)
    adapter = dict(zip(TVBS_HOSTS, (_news, _cars, _supertaste)))[host]
    options = {'adapter_contract_version': adapter_contract_version} if host == TVBS_HOSTS[0] else {}
    heading, body, cleaned, author, basis = adapter(soup, article, clocks, notices, original, **options)
    _visible_tree(heading)
    _visible(body)
    title = heading.get_text(' ', strip=True)
    suffix = {'news.tvbs.com.tw': '', 'cars.tvbs.com.tw': ' | 地球黃金線', 'supertaste.tvbs.com.tw': ' | 食尚玩家'}[host]
    ogtitle = _one(soup, 'meta[property="og:title"][content]')
    schema_title, social_title = article.get('headline'), ogtitle.get('content')
    # Publishers edit SEO/social headlines independently of the visible report.
    # Require a native title anchor, never fuzzy similarity or a neighbouring
    # headline. Keep both original declarations when exactly one differs.
    if (not title or not _key(schema_title) or not _key(social_title)
            or (_key(title) != _key(schema_title) and _key(title + suffix) != _key(social_title))):
        raise NewsError('article_identity_not_verified')
    if _key(title) != _key(schema_title) or _key(title + suffix) != _key(social_title):
        notices += [{'kind': 'native_schema_headline', 'text': schema_title},
            {'kind': 'native_OG_headline', 'text': social_title},
            {'kind': 'native_visible_headline', 'text': title}]
    if cleaned.select('script, form, article, [rel~="license"]'):
        raise NewsError('article_rights_or_scope_excluded')
    captions = [node.get_text(' ', strip=True) for node in body.select('figure figcaption')]
    for caption in body.select('figure figcaption'):
        _visible_tree(caption)
    for node in cleaned.find_all(True):
        if not node.find_parent('figure'):
            _visible(node)
    if any(_EXTERNAL_CREDIT.match(node.get_text(' ', strip=True)) for node in cleaned.select(':scope > p')):
        raise NewsError('article_rights_or_scope_excluded')
    # Normalize the small verified DOM wrapper, not a response or observation.
    # Raw HTML stays immutable. Common text extraction still reads its source
    # nodes once, excluding image binaries/captions and non-body tags.
    wrapper = BeautifulSoup('', 'lxml').new_tag('div')
    cleaned['class'] = ['tvbs-native-body']
    wrapper.append(cleaned)
    authors = author if isinstance(author, list) else [author]
    attribution = {'native_author_name': authors[0]['name'],
        'source_author_account': authors[0].get('url') or authors[0].get('@id'), 'author_name_invented': False}
    if len(authors) == 2:
        attribution['publisher_ordered_authors_with_responsible_editor'] = authors
    anomaly = clocks['dateModified'] < clocks['datePublished']
    clock_basis = ('source_declared_aware_datetimes_match_native_release_revision_minutes' if host == TVBS_HOSTS[0]
        else 'source_declared_aware_datetimes_match_native_release_date_datetime' if host == TVBS_HOSTS[1]
        else 'source_declared_aware_datetimes_match_meta; native_visible_release_clock_not_present')
    if anomaly:
        clock_basis += '; source_declared_revision_precedes_release_exactly_1_second_same_visible_minute; raw_clocks_preserved_not_corrected'
    return {'headline': title, 'author': authors, 'datePublished': article['datePublished'],
        'dateModified': article['dateModified'], 'article_identity_basis': 'canonical_OG_single_schema_visible_heading_with_native_title_anchor_' + basis + '_v1',
        'author_attribution_detail_json': json.dumps(attribution, ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False),
        'published_time_basis': clock_basis + '; not_observed_historical_PIT',
        'body_completeness': 'verified_current_whole_native_report_prose; original_paragraphs_headings_and_risk_notices_retained; captions_and_native_info_cards_recorded_separately; advertising_related_stories_and_UI_excluded'}, wrapper


def tvbs_validation_metadata(soup, *, page_url):
    """Bounded native failure facts; never body/credit/title retention or proof.

    The owner reserves its existing fixed daily budget before this function.
    A probe may also use it on its one already-read response. Neither caller
    receives an article, permission, retry change or another HTTP request.
    """
    facts = {'contract_version': 1, 'schema_valid': False,
        'body_text_retained': False, 'raw_scope_or_article_acceptance_granted': False}
    try:
        article, host, _ = _schema(soup, page_url)
    except (NewsError, ValueError, TypeError, AttributeError, RecursionError):
        return facts
    compact_hash = lambda value: hashlib.sha256(_key(value).encode()).hexdigest() if isinstance(value, str) else None
    safe_clock = lambda value: value if isinstance(value, str) and re.fullmatch(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]{1,6})?(?:Z|\+08:00)', value) else None
    authors = article.get('author')
    authors = authors if isinstance(authors, list) else [authors]
    labels = soup.select('main > div > div > header li') if host == TVBS_HOSTS[0] else []
    clocks, credits = [], []
    for node in labels[:16]:
        text = node.get_text(' ', strip=True)
        if re.fullmatch(r'(?:發佈時間|發布時間|最後更新時間)：\s*[0-9]{4}[.][0-9]{2}[.][0-9]{2} [0-9]{2}:[0-9]{2}', text):
            clocks.append(text)
            continue
        role = re.match(r'^(記者|編輯|編譯|作者)：', _key(text))
        credits.append({'kind': role[1] if role else 'unknown',
            'responsible_editor_present': '｜責任編輯：' in _key(text),
            'credit_compact_sha256': compact_hash(text)})
    heading = soup.select('main > div > div > header > h1')
    facts.update(schema_valid=True, host=host,
        declared_publication=safe_clock(article.get('datePublished')),
        declared_modification=safe_clock(article.get('dateModified')),
        declared_author_count=len(authors),
        declared_author_types=[a.get('@type') if isinstance(a, dict) and isinstance(a.get('@type'), str) and a.get('@type') in {'Person', 'Organization'}
            else 'unknown' for a in authors[:8]],
        declared_author_name_compact_sha256=[compact_hash(a.get('name')) if isinstance(a, dict) else None for a in authors[:8]],
        declared_external_author_present=any(isinstance(a, dict) and isinstance(a.get('name'), str) and a.get('name') in _EXTERNAL_AUTHORS | {'品觀點'} for a in authors[:8]),
        authors_truncated=len(authors) > 8, header_label_count=len(labels), header_labels_truncated=len(labels) > 16,
        native_clock_labels=clocks, native_credit_shapes=credits,
        native_heading_compact_sha256=[compact_hash(n.get_text(' ', strip=True)) for n in heading[:4]],
        declared_headline_compact_sha256=compact_hash(article.get('headline')),
        og_headline_compact_sha256=[compact_hash(n.get('content')) for n in soup.select('meta[property="og:title"][content]')[:4]],
        native_original_body_container_count=len(soup.select('.article-editor-content, .gap-article-section-gap, article#article-content')),
        configured_wrapper_is_created_by_adapter_not_present_in_original=True)
    return facts
