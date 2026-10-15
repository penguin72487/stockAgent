"""Reviewed regional public page layouts, not a separate HTTP collector.

An agency's open-information declaration is not inherited by a newspaper on a
government hostname. These adapters verify only the current public page's
identity, visible text and credits; rights and access remain source policies.
"""
from __future__ import annotations

from copy import deepcopy
import json
import re
from datetime import datetime
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from downloader.news_fulltext_policy import NewsError, normalize_url


_TAINAN_NEWS_PATH = "/news_content.aspx"
_TAINAN_REVIEWED_RELEASE_UNITS = frozenset({"新聞及國際關係處", "社會局"})
_TAINAN_RELEASE_FIELD = re.compile(r"(?<!\S)(發稿(?:單位|時間|人))[：:]\s*")

_MACAU_REVIEWED_AGENCY = ("https://www.gov.mo/zh-hans/entity-page/entity-301/", "劳工事务局（DSAL）")

_TAICHUNG_REVIEWED_CONTACT_UNITS = (
    '台中市政府', '台中市政府文化局表演藝術科', '台中市政府環境保護局',
    '台中市政府農業局', '台中市政府研考會', '台中市政府教育局',
    '台中市政府建設局', '台中市政府運動局', '台中市政府教育局學生事務室',
    '台中市政府研究發展考核委員會', '台中市政府社會局人民團體科',
    '台中市政府農業局作物生產科', '台中市政府民政局', '台中市政府經濟發展局',
    '台中市政府新聞局', '台中市政府運動局競技運動科', '台中市政府社會局',
    '台中市政府交通局', '台中市政府消防局危險物品管理科',
    '台中市中興地政事務所', '台中市大甲區戶政事務所',
    '台中市大甲地政事務所', '台中政府運動局',
)
_TAICHUNG_CONTACT_UNITS = tuple(sorted({
    *_TAICHUNG_REVIEWED_CONTACT_UNITS,
    *(unit.replace('台中', '臺中', 1) for unit in _TAICHUNG_REVIEWED_CONTACT_UNITS),
}, key=lambda unit: (-len(unit), unit)))


def _taichung_contact_unit(value):
    """Keep labelled organization/contact roles, including an observed typo.

    Longest exact reviewed unit prevents 張小姐 being included in an agency
    name when a source omits the space. Never expand an office abbreviation,
    correct 台中政府運動局, or turn a contact person into a named reporter.
    """
    match = re.fullmatch(r'聯絡人[:：]\s*([^\n]+)', value)
    if match:
        raw = match[1].strip()
        for unit in _TAICHUNG_CONTACT_UNITS:
            if not raw.startswith(unit):
                continue
            remainder = raw[len(unit):]
            if (not remainder or remainder[0].isspace()
                    or re.fullmatch(r'[\u3400-\u9fff]{1,4}(?:先生|小姐|科長|主任|課督|科員)', remainder)):
                return unit, raw, remainder.strip() or None
    raise NewsError('article_rights_or_scope_excluded')


def _release_day(value, pattern, format):
    """Validate declared date precision, without inventing seconds/timezone."""
    try:
        if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
            raise ValueError('unreviewed publication precision')
        return datetime.strptime(value, format).date()
    except (ValueError, TypeError):
        raise NewsError('invalid_bulk_publication_clock') from None


def _release_heading(value):
    return re.sub(r'\s+', '', value) if isinstance(value, str) else ''


def _taichung_identity(url):
    try:
        parts = urlsplit(url)
    except (ValueError, TypeError):
        return None
    if parts.scheme != 'https' or parts.netloc != 'www.taichung.gov.tw' or parts.query or parts.fragment:
        return None
    match = re.fullmatch(r'/(?:([1-9][0-9]*)/post|8868/8872/9962/([1-9][0-9]*))', parts.path)
    return next((value for value in match.groups() if value), None) if match else None


def _taichung_absent_canonical_identity(scope, *, title, page_url):
    """Observed native OG-only response: verify its declared share pointers.

    Never fetch social URLs or manufacture a canonical link. This variant
    needs the exact news9962 breadcrumb plus all three current-page pointers;
    a missing/competing/foreign pointer is not an accepted identity.
    """
    from downloader.news_fulltext_public_originals import _one, _visible

    if not re.fullmatch(r'https://www[.]taichung[.]gov[.]tw/[1-9][0-9]*/post', page_url):
        raise NewsError('article_identity_not_verified')
    friendly = _one(scope, ':scope > section.friendly')
    crumb = _one(friendly, 'a[href="/9962/Lpsimplelist"]')
    if crumb.get_text(' ', strip=True) != '市政新聞':
        raise NewsError('article_identity_not_verified')
    share = _one(scope, ':scope > section.share')
    links = share.select('a[href]')
    expected = {
        ('www.facebook.com', '/sharer.php'): {'u': page_url},
        ('twitter.com', '/share'): {'url': page_url, 'text': title},
        ('social-plugins.line.me', '/lineit/share'): {'url': page_url, 'text': title, 'from': 'line_scheme'},
    }
    observed = set()
    for node in links:
        try:
            parts = urlsplit(node['href'])
            pairs = parse_qsl(parts.query, keep_blank_values=True)
        except (TypeError, ValueError):
            raise NewsError('article_identity_not_verified') from None
        key = (parts.netloc, parts.path)
        wanted = expected.get(key)
        values = dict(pairs)
        if (parts.scheme != 'https' or parts.fragment or wanted is None or key in observed
                or len(pairs) != len(wanted) or set(values) != set(wanted)
                or any(_release_heading(values[name]) != _release_heading(value)
                       if name == 'text' else values[name] != value for name, value in wanted.items())):
            raise NewsError('article_identity_not_verified')
        _visible(node)
        observed.add(key)
    if observed != set(expected) or len(links) != 3:
        raise NewsError('article_identity_not_verified')
    for node in (friendly, crumb, share):
        _visible(node)


def taichung_visible_news(soup, *, page_url):
    """Current municipal release ID, agency contact unit and date-only panel.

    Agency attribution is an organization, not a reporter inferred from a
    contact person. Keep public business contact fields out of narrative;
    immutable raw retains their original roles. Media/captions are excluded.
    """
    from downloader.news_fulltext_public_originals import _one, _visible

    identity = _taichung_identity(page_url)
    canonicals = soup.select('link[rel~="canonical"]')
    og = _one(soup, 'meta[property="og:url"][content]')
    if (identity is None or len(canonicals) > 1
            or canonicals and canonicals[0].get('href') != og.get('content')
            or _taichung_identity(og['content']) != identity
            or not og['content'].startswith('https://www.taichung.gov.tw/8868/8872/9962/')):
        raise NewsError('article_identity_not_verified')
    scope = _one(soup, 'main#center')
    heading = _one(scope, ':scope > header > h2')
    title = heading.get_text(' ', strip=True)
    declared_title = _one(soup, 'meta[property="og:title"][content]')['content']
    if not title or _release_heading(title) != _release_heading(declared_title):
        raise NewsError('article_identity_not_verified')
    if not canonicals:
        _taichung_absent_canonical_identity(scope, title=title, page_url=page_url)
    body = _one(scope, ':scope > article#cpArticle.cpArticle')
    panel = _one(scope, ':scope > section.pubInfo', 'article_attribution_not_verified')
    dates = []
    for field in panel.select(':scope > ul > li'):
        values = field.find_all('em', recursive=False)
        labels = ''.join(str(node) for node in field.contents if getattr(node, 'name', None) is None).strip()
        if labels == '發布日期：':
            if len(values) != 1:
                raise NewsError('article_attribution_not_verified')
            dates.append(values[0].get_text(' ', strip=True))
    if len(dates) != 1:
        raise NewsError('article_attribution_not_verified')
    _release_day(dates[0], r'[0-9]{4}-[0-9]{2}-[0-9]{2}', '%Y-%m-%d')
    paragraphs = [node for node in body.find_all('p', recursive=False) if node.get_text(' ', strip=True)]
    if len(paragraphs) < 3:
        raise NewsError('article_attribution_not_verified')
    contact, phone = paragraphs[-2:]
    unit, raw_contact, person = _taichung_contact_unit(contact.get_text(' ', strip=True))
    if re.fullmatch(r'聯絡電話[:：]\s*[^\n]+', phone.get_text(' ', strip=True)) is None:
        raise NewsError('article_rights_or_scope_excluded')
    # The unit is explicitly present in the release's labelled contact field.
    # No named author or department abbreviation is reconstructed from prose.
    for node in (scope, heading, body, panel, contact, phone):
        _visible(node)
    for root in (heading, panel, body):
        for node in root.find_all(True):
            if not node.find_parent(['figure', 'figcaption']) and node.name not in {'figure', 'figcaption', 'img'}:
                _visible(node)
    if body.select('[rel~="license"], [data-paywall], form, .paywall'):
        raise NewsError('article_rights_or_scope_excluded')
    # One bounded body copy, not another whole-page serialization/parse.
    copied = deepcopy(body)
    copied_paragraphs = [node for node in copied.find_all('p', recursive=False) if node.get_text(' ', strip=True)]
    for node in copied_paragraphs[-2:]:
        node.decompose()
    text_scope = soup.new_tag('main')
    text_scope.append(copied)
    return {
        'headline': title, 'author': [{'@type': 'Organization', 'name': unit}],
        'author_attribution_detail_json': json.dumps({'release_contact_organization_raw': unit,
            'release_contact_raw': raw_contact, 'source_contact_person_raw': person,
            'role': 'publisher_labelled_agency_contact_unit; not_named_reporter',
            'named_author_invented': False, 'contact_fields_excluded_from_narrative': True}, ensure_ascii=False),
        'datePublished': dates[0],
        'published_time_basis': 'publisher_declared_date_only; no_intraday_time_or_timezone_inferred',
        'article_identity_basis': ('same_main_host_news9962_canonical_og_ID_visible_headline_and_native_release_panel_v1'
            if canonicals else 'same_main_host_news9962_og_ID_visible_headline_breadcrumb_three_declared_share_URL_witnesses_native_panel_v2; canonical_not_invented'),
        'body_completeness': 'verified_current_native_release_prose; labelled_business_contacts_and_media_excluded',
    }, text_scope


def _changhua_identity(url):
    try:
        parts = urlsplit(url)
        pairs = parse_qsl(parts.query, keep_blank_values=True)
    except (ValueError, TypeError):
        return None
    if (parts.scheme != 'https' or parts.netloc != 'www.chcg.gov.tw' or parts.fragment
            or parts.path != '/ch2/newsdetail.aspx' or len(pairs) != 1 or pairs[0][0] != 'bull_id'
            or re.fullmatch(r'[1-9][0-9]*', pairs[0][1]) is None):
        return None
    return pairs[0][1]


def changhua_visible_news(soup, *, page_url):
    """Native form/breadcrumb/print ID and explicitly labelled release panel."""
    from urllib.parse import urljoin
    from downloader.news_fulltext_public_originals import _one, _visible

    identity = _changhua_identity(page_url)
    form = _one(soup, 'form#form1[action]')
    if identity is None or _changhua_identity(urljoin(page_url, form['action'])) != identity:
        raise NewsError('article_identity_not_verified')
    scope = _one(form, '#print_parts')
    heading = _one(scope, ':scope > .ipdb-title-block > .ipdb-title')
    copy = deepcopy(heading)
    marker = copy.find('span', recursive=False)
    if marker is not None:
        if (marker.get_text() != '(歷史訊息)' or marker.attrs != {'style': 'color:red;'}
                or marker.find(True) is not None):
            raise NewsError('article_identity_not_verified')
        marker.decompose()
    title = copy.get_text(' ', strip=True)
    declared = _one(soup, 'meta[property="og:title"][content]')['content']
    breadcrumbs = soup.select('a.active[href]')
    matches = [node for node in breadcrumbs if _changhua_identity(urljoin(page_url, node['href'])) == identity
               and _release_heading(node.get_text(' ', strip=True)) == _release_heading(title)]
    print_links = scope.select('a[href^="print_all.aspx?"]')
    if (not title or _release_heading(title) != _release_heading(declared) or len(matches) != 1
            or len(print_links) != 1 or print_links[0]['href'] != 'print_all.aspx?bull_id=' + identity):
        raise NewsError('article_identity_not_verified')
    # This observed native template does not declare a canonical/og URL or
    # NewsArticle schema. Unexpected competing representations need review.
    if soup.select('link[rel~="canonical"], meta[property="og:url"], script[type="application/ld+json"]'):
        raise NewsError('article_identity_not_verified')
    panel = _one(scope, ':scope > .active-detail-list-block:not(.introduce)', 'article_attribution_not_verified')
    fields = {}
    for item in panel.select(':scope > ul > li'):
        label = _one(item, ':scope > .t', 'article_attribution_not_verified').get_text(' ', strip=True)
        value = _one(item, ':scope > .c', 'article_attribution_not_verified').get_text(' ', strip=True)
        if label in fields:
            raise NewsError('article_attribution_not_verified')
        fields[label] = value
    if set(fields) != {'發布單位', '發布日期', '聯絡資訊'} or fields['發布單位'] != '經濟暨綠能發展處':
        raise NewsError('article_rights_or_scope_excluded')
    period = fields['發布日期'].split('~')
    if len(period) != 2:
        raise NewsError('invalid_bulk_publication_clock')
    start, end = (_release_day(value, r'[0-9]{4}/[0-9]{2}/[0-9]{2}', '%Y/%m/%d') for value in period)
    header_day = _one(scope, ':scope > .ipdb-title-block .d-v > .date').get_text(' ', strip=True)
    if header_day != period[0] or end < start:
        raise NewsError('invalid_bulk_publication_clock')
    body = _one(scope, ':scope > article.content')
    for node in (form, scope, heading, matches[0], print_links[0], panel, body):
        _visible(node)
    for root in (heading, panel, body):
        for node in root.find_all(True):
            _visible(node)
    if body.select('[rel~="license"], [data-paywall], form, .paywall'):
        raise NewsError('article_rights_or_scope_excluded')
    return {
        'headline': title, 'author': [{'@type': 'Organization', 'name': fields['發布單位']}],
        'author_attribution_detail_json': json.dumps({'release_unit_raw': fields['發布單位'],
            'role': 'publisher_labelled_release_organization; not_named_reporter',
            'publication_panel_raw': fields['發布日期'], 'header_publication_day_raw': header_day,
            'named_author_invented': False}, ensure_ascii=False),
        'verified_article_notices_json': json.dumps([{'kind': 'publisher_declared_release_period',
            'text': fields['發布日期'], 'source_links': [], 'image_sources': []}], ensure_ascii=False),
        'datePublished': header_day,
        'published_time_basis': 'publisher_header_date_only_matching_release_period_start; full_interval_preserved; no_intraday_time_inferred',
        'article_identity_basis': 'same_main_host_bull_id_form_breadcrumb_print_ID_visible_og_headline_native_release_panel_v1',
        'body_completeness': 'verified_current_native_government_release_text; separate_contact_and_media_blocks_excluded',
    }, scope


def macau_visible_news(soup, *, page_url):
    """One declared official release, not sidebar dates or hidden schema text.

    The reviewed page's schema adds +00:00 to the visible naive wall clock,
    without establishing that visible clock's timezone. Retain it verbatim,
    not an inferred UTC or Macau offset. Preserve the exact schema in raw.
    The portal grant excludes third-party works, so only the reviewed, visibly
    credited labour department is eligible; a .gov.mo host is not permission.
    """
    from downloader.news_fulltext_public_originals import _visible
    from downloader.news_fulltext_policy import _objects

    try:
        parts = urlsplit(page_url)
    except (ValueError, TypeError):
        raise NewsError("article_identity_not_verified") from None
    if (parts.scheme != "https" or parts.netloc != "www.gov.mo" or parts.query or parts.fragment
            or not re.fullmatch(r"/zh-hans/news/[1-9][0-9]*/", parts.path)):
        raise NewsError("article_identity_not_verified")
    scopes = soup.select("main#content .mce-content-body")
    canonical = soup.select('link[rel~="canonical"]')
    declared = soup.select('meta[property="og:url"]')
    titles = soup.select('meta[property="og:title"]')
    if (len(scopes) != 1 or len(canonical) != 1 or len(declared) != 1 or len(titles) != 1
            or canonical[0].get("href") != page_url or declared[0].get("content") != page_url):
        raise NewsError("article_identity_not_verified")
    scope = scopes[0]
    heads = scope.select(":scope > .news-title > h1")
    panels = scope.select(":scope > div > aside.metadata-list > dl")
    bodies = scope.select(":scope > article")
    if (len(heads) != 1 or len(panels) != 1 or len(bodies) != 1
            or not heads[0].get_text(" ", strip=True)
            or heads[0].get_text(" ", strip=True) != titles[0].get("content")):
        raise NewsError("article_identity_not_verified")
    for node in (scope, heads[0], panels[0], bodies[0]):
        _visible(node)
    for root in (heads[0], panels[0]):
        for node in root.find_all(True):
            _visible(node)
    fields = {}
    for label in panels[0].find_all("dt", recursive=False):
        name = label.get_text(" ", strip=True)
        value = label.find_next_sibling("dd")
        if name in fields or value is None:
            raise NewsError("article_attribution_not_verified")
        _visible(label)
        _visible(value)
        fields[name] = value
    if set(fields) != {"来源：", "发布日期："}:
        raise NewsError("article_attribution_not_verified")
    credits = fields["来源："].select("a[href]")
    if (len(credits) != 1 or credits[0].get("href") != _MACAU_REVIEWED_AGENCY[0]
            or credits[0].get_text(" ", strip=True) != _MACAU_REVIEWED_AGENCY[1]):
        raise NewsError("article_rights_or_scope_excluded")
    _visible(credits[0])
    clocks = fields["发布日期："].select("time[datetime]")
    if len(clocks) != 1:
        raise NewsError("article_attribution_not_verified")
    _visible(clocks[0])
    raw_clock = clocks[0]["datetime"]
    visible_clock = re.fullmatch(r"([0-9]{4})年([0-9]{1,2})月([0-9]{1,2})日 ([0-9]{1,2}):([0-9]{2})",
                                 clocks[0].get_text(" ", strip=True))
    try:
        if (not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", raw_clock)
                or visible_clock is None):
            raise ValueError("unreviewed visible clock")
        clock = datetime.strptime(raw_clock, "%Y-%m-%d %H:%M:%S")
        if tuple(map(int, visible_clock.groups())) != (clock.year, clock.month, clock.day, clock.hour, clock.minute):
            raise ValueError("visible clock mismatch")
    except (ValueError, TypeError):
        raise NewsError("invalid_bulk_publication_clock") from None
    schemas = []
    try:
        for script in scope.select(':scope > script[type="application/ld+json"]'):
            schemas.extend(node for node in _objects(json.loads(script.get_text())) if node.get("@type") == "NewsArticle")
    except (ValueError, TypeError, RecursionError):
        raise NewsError("article_identity_not_verified") from None
    if len(schemas) != 1:
        raise NewsError("article_identity_not_verified")
    schema = schemas[0]
    if str(schema.get("isAccessibleForFree", "true")).lower() == "false":
        raise NewsError("paywall_or_login")
    identity = schema.get("mainEntityOfPage")
    author = schema.get("author")
    if (not isinstance(identity, dict) or identity.get("@id") != page_url
            or schema.get("headline") != heads[0].get_text(" ", strip=True)
            or not isinstance(author, dict) or author.get("@type") != "Organization"
            or author.get("name") != _MACAU_REVIEWED_AGENCY[1]):
        raise NewsError("article_identity_not_verified")
    try:
        schema_clock = datetime.fromisoformat(schema.get("datePublished", "").replace("Z", "+00:00"))
        if schema_clock.tzinfo is None or schema_clock.replace(tzinfo=None) != clock:
            raise ValueError("schema wall clock mismatch")
    except (ValueError, TypeError, AttributeError):
        raise NewsError("invalid_bulk_publication_clock") from None
    for node in bodies[0].find_all(True):
        # Galleries are separate media, never fetched or interpreted as text.
        if not node.find_parent(class_="gallery") and "gallery" not in node.get("class", []):
            _visible(node)
    return {
        "headline": heads[0].get_text(" ", strip=True),
        "author": [{"name": _MACAU_REVIEWED_AGENCY[1]}],
        "datePublished": raw_clock,
        "published_time_basis": "publisher_visible_naive_datetime; matching_schema_wall_clock; no_timezone_inferred; exact_schema_in_raw",
        "article_identity_basis": "matching_portal_canonical_og_visible_headline_native_schema_and_reviewed_department_v1",
        "body_completeness": "verified_current_government_news_text_with_visible_caption; media_not_downloaded",
    }, scope


def _tainan_news_path(path: str) -> bool:
    """Observed ASP namespace only; Unicode lookalikes are not aliases."""
    return path.isascii() and path.lower() == _TAINAN_NEWS_PATH


def tainan_news_request_url(url: str) -> str:
    """Repair the verified HTML-escaped separator, keeping frontier identity.

    The original n=13370&amp;s=<id> returned 410, whereas the same official
    n/s article returned 200 and declared its identity. This is deliberately
    not global html.unescape, host substitution, parameter sorting, or a
    retry after access denial. Ambiguous/extra/signed parameters stay intact.
    """
    parts = urlsplit(url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    if (parts.scheme == "https" and parts.netloc == "www.tainan.gov.tw"
            and _tainan_news_path(parts.path) and len(pairs) == 2
            and pairs[0] == ("n", "13370") and pairs[1][0] == "amp;s"
            and re.fullmatch(r"[1-9][0-9]*", pairs[1][1])):
        return urlunsplit((parts.scheme, parts.netloc, parts.path,
                           "n=13370&s=" + pairs[1][1], parts.fragment))
    return url


def _tainan_identity(url: str):
    try:
        parts = urlsplit(normalize_url(url))
        pairs = parse_qsl(parts.query, keep_blank_values=True)
    except (NewsError, ValueError, TypeError):
        return None
    values = dict(pairs)
    if (parts.scheme != "https" or parts.netloc != "www.tainan.gov.tw"
            or not _tainan_news_path(parts.path) or len(values) != len(pairs)
            or set(values) not in ({"n", "s"}, {"n", "s", "Create"})
            or values.get("n") != "13370" or not re.fullmatch(r"[1-9][0-9]*", values.get("s", ""))
            or "Create" in values and values["Create"] != "1"):
        return None
    return values["n"], values["s"]


def _headline_key(value: str) -> str:
    # The verified KMDN heading styles its initial characters in a span. Text
    # separators are layout, not another article identity or another headline.
    return re.sub(r"\s+", "", value).lstrip("-") if isinstance(value, str) else ""


def tainan_visible_news(soup, *, page_url):
    """One official release, matched n/s, visible agency credit and ROC clock."""
    from downloader.news_fulltext_public_originals import _visible

    identity = _tainan_identity(page_url)
    declared = soup.select('meta[property="og:url"]')
    scopes = soup.select("#CCMS_Content")
    if (identity is None or len(declared) != 1
            or _tainan_identity(declared[0].get("content")) != identity or len(scopes) != 1):
        raise NewsError("article_identity_not_verified")
    scope = scopes[0]
    heads = scope.select(".simple-text.title h3")
    bodies = scope.select(".area-essay.page-caption-p .essay .p")
    titles = soup.select('meta[property="og:title"]')
    if (len(heads) != 1 or len(bodies) != 1 or len(titles) != 1
            or not heads[0].get_text(" ", strip=True)
            or _headline_key(heads[0].get_text(" ", strip=True)) != _headline_key(titles[0].get("content", ""))):
        raise NewsError("article_identity_not_verified")
    for node in (heads[0], bodies[0]):
        _visible(node)
    # Malformed nested paragraphs are source layout, not independent credits.
    # Actual 7920395 puts two labelled fields in ONE leaf paragraph. Split only
    # leading source-labelled headers, never narrative, captions or footer text.
    leaves = [n for n in bodies[0].find_all("p")
              if not n.find("p") and not n.find_parent(["figure", "figcaption", "nav", "aside"])]
    paragraphs = [n.get_text(" ", strip=True) for n in leaves]
    fields = {label: set() for label in ("發稿單位", "發稿時間", "發稿人")}
    for node, text in zip(leaves[:8], paragraphs[:8], strict=True):
        labels = list(_TAINAN_RELEASE_FIELD.finditer(text))
        if not labels or labels[0].start() != 0:
            continue
        _visible(node)
        for inline in node.find_all(True):
            _visible(inline)
        for position, label in enumerate(labels):
            end = labels[position + 1].start() if position + 1 < len(labels) else len(text)
            value = text[label.end():end].strip()
            if value:
                fields[label[1]].add(value)
    units, clocks, people = (fields[label] for label in ("發稿單位", "發稿時間", "發稿人"))
    if len(units) != 1 or not units <= _TAINAN_REVIEWED_RELEASE_UNITS:
        raise NewsError("article_rights_or_scope_excluded")
    if len(clocks) != 1:
        raise NewsError("article_attribution_not_verified")
    if not any(text and not re.match(r"發稿(?:單位|時間|人)[：:]", text) for text in paragraphs):
        raise NewsError("body_not_found")
    clock = next(iter(clocks))
    match = re.fullmatch(r"([0-9]{2,3})年([0-9]{1,2})月([0-9]{1,2})日\s*([0-9]{1,2})[:：]([0-9]{2})(?:\(([0-9]{1,2})[:：]([0-9]{2})更新\))?", clock)
    if match is None:
        raise NewsError("invalid_bulk_publication_clock")
    try:
        year, month, day, hour, minute = map(int, match.groups()[:5])
        datetime(year + 1911, month, day, hour, minute)  # Validate, don't rewrite raw source clock.
        if match[6] is not None:
            datetime(year + 1911, month, day, int(match[6]), int(match[7]))
    except ValueError:
        raise NewsError("invalid_bulk_publication_clock") from None
    return {
        "headline": heads[0].get_text(" ", strip=True),
        "author": [{"name": name} for name in sorted(units) + sorted(people)],
        "datePublished": clock,
        "published_time_basis": "publisher_declared_ROC_clock_and_revision_text; no_timezone_or_missing_seconds_inferred",
        "article_identity_basis": "matched_official_news_ASCII_case_insensitive_ASP_path_query_and_visible_agency_release_v2",
        "body_completeness": "verified_current_article_container",
    }, scope


def _kmdn_identity(url: str, *, allow_student_essay: bool = False):
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, ValueError, TypeError):
        return None
    pattern = (r"/1117/1271/(1272|1277|287081)/([1-9][0-9]*)/?" if allow_student_essay else
               r"/1117/1271/(1272|287081)/([1-9][0-9]*)/?")
    match = re.fullmatch(pattern, parts.path)
    if parts.scheme != "https" or parts.netloc != "www.kmdn.gov.tw" or parts.query or not match:
        return None
    return match[1], match[2]


def kmdn_visible_article(soup, *, page_url, allow_student_essay: bool = False):
    """Current heading/panel/share identity, never stale site-wide og:url.

    The four real originals declare a stale og:url (262893 or 261387). Each
    current function panel independently shares its actual article URL through
    Facebook and Twitter. Verify both local links without requesting either
    social network; keep stale source metadata in the exact raw response.
    Only the reviewed v2 capture contract admits the native 1277 student
    contribution namespace. V1 and unrelated paths retain their original scope.
    """
    identity = _kmdn_identity(page_url, allow_student_essay=allow_student_essay)
    scopes = soup.select("#center > .container")
    if identity is None or len(scopes) != 1:
        raise NewsError("article_identity_not_verified")
    scope = scopes[0]
    heads = scope.select(":scope > h2.title")
    panels = scope.select(":scope > .function_panel")
    bodies = scope.select(":scope > section.cp")
    titles = soup.select('meta[property="og:title"]')
    if (len(heads) != 1 or len(panels) != 1 or len(bodies) != 1 or len(titles) != 1
            or not heads[0].get_text("", strip=True)
            or _headline_key(heads[0].get_text("", strip=True)) != _headline_key(titles[0].get("content", ""))):
        raise NewsError("article_identity_not_verified")
    shares = {"facebook": [], "twitter": []}
    for link in panels[0].select(".share a[href]"):
        try:
            parts = urlsplit(link["href"])
        except ValueError:
            raise NewsError("article_identity_not_verified") from None
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        if parts.hostname == "www.facebook.com" and parts.path == "/share.php":
            shares["facebook"].extend(_kmdn_identity(value, allow_student_essay=allow_student_essay)
                                      for key, value in pairs if key == "u")
        elif parts.hostname == "twitter.com" and parts.path == "/share":
            shares["twitter"].extend(_kmdn_identity(value, allow_student_essay=allow_student_essay)
                                     for key, value in pairs if key == "url")
    if any(values != [identity] for values in shares.values()):
        raise NewsError("article_identity_not_verified")
    bylines = []
    for row in panels[0].select(":scope > .click_rate"):
        labels = row.find_all("span", recursive=False)
        if len(labels) == 2 and labels[0].get_text(" ", strip=True) in {"記者：", "作者："}:
            bylines.append(labels[1].get_text(" ", strip=True))
    if len(bylines) != 1 or not bylines[0]:
        raise NewsError("article_attribution_not_verified")
    times = panels[0].select(":scope > .publish_time > time")
    clock = times[0].get_text(" ", strip=True) if len(times) == 1 else None
    if clock is None or not re.fullmatch(r"[0-9]{4}/[0-9]{2}/[0-9]{2}", clock):
        raise NewsError("invalid_bulk_publication_clock")
    try:
        datetime.strptime(clock, "%Y/%m/%d")
    except ValueError:
        raise NewsError("invalid_bulk_publication_clock") from None
    return {
        "headline": heads[0].get_text("", strip=True), "author": [{"name": bylines[0]}],
        "datePublished": clock,
        "published_time_basis": "publisher_declared_date_only; no_intraday_time_inferred",
        "article_identity_basis": "current_function_panel_two_source_share_urls_and_visible_heading_v1; source_og_url_untrusted",
        "body_completeness": "verified_current_article_container",
    }, scope


_COOLLOUD_COPYRIGHT = 'https://www.coolloud.org.tw/node/4524'
_COOLLOUD_LICENSE = 'http://creativecommons.org/licenses/by-nc-sa/2.0/tw/deed.zh_TW'


def coolloud_visible_original(soup, *, page_url):
    """Native Drupal staff report, not external archives or open submissions.

    The actual source's <h1><p> title is malformed HTML: lxml places its p
    immediately after an empty h1. Admit only those two observed shapes,
    bounded by the same native region and matching OG/document titles. The
    linked site notice licenses publisher originals, not third-party works.
    Calendar/event date, Facebook account and chief editor never replace the
    source publication clock or staff reporter. No HTTP is performed here.
    """
    from urllib.parse import quote
    from downloader.news_fulltext_public_originals import _one, _visible

    try:
        parts = urlsplit(page_url)
        identity = re.fullmatch(r'/node/([1-9][0-9]*)', parts.path)
    except (ValueError, TypeError):
        identity = None
    if (identity is None or parts.scheme != 'https' or parts.netloc != 'www.coolloud.org.tw'
            or parts.query or parts.fragment):
        raise NewsError('article_identity_not_verified')
    page = _one(soup, '.main-container > .row > section.col-sm-12')
    region = _one(page, ':scope > .region.region-content')
    article = _one(region, ':scope > article.node')
    if article.get('id') != 'node-' + identity[1]:
        raise NewsError('article_identity_not_verified')
    classes = set(article.get('class', []))
    if ('node-post' not in classes or classes & {'node-news', 'node-outer'}
            or article.select('.field-name-field-reminder')):
        raise NewsError('article_rights_or_scope_excluded')
    for selector, attribute in (('head link[rel~="canonical"]', 'href'),
                                ('head link[rel~="shortlink"]', 'href'),
                                ('head meta[property="og:url"]', 'content')):
        if _one(soup, selector).get(attribute) != page_url:
            raise NewsError('article_identity_not_verified')
    heading = _one(page, ':scope > h1.page-header')
    heading_text = heading.get_text(' ', strip=True)
    if heading_text:
        native_p = _one(heading, ':scope > p')
        if len(heading.find_all(True, recursive=False)) != 1 or heading.find_next_sibling() is not region:
            raise NewsError('article_identity_not_verified')
    else:
        native_p = heading.find_next_sibling()
        if (heading.contents or native_p is None or native_p.name != 'p' or native_p.attrs
                or native_p.find_next_sibling() is not region):
            raise NewsError('article_identity_not_verified')
    title = native_p.get_text(' ', strip=True)
    if (not title or heading_text and heading_text != title
            or _one(soup, 'head meta[property="og:title"]').get('content') != title
            or _one(soup, 'head > title').get_text(' ', strip=True) != title + ' | 苦勞網'):
        raise NewsError('article_identity_not_verified')
    info = _one(article, ':scope > .group-info')
    source = _one(info, '.field-name-field-source > .field-items > .field-item > a[href]')
    credit = _one(article, ':scope > .group-author')
    reporter = _one(credit, '.field-name-field-author > .field-items > .field-item > a[href]',
                    'article_attribution_not_verified')
    staff = _one(credit, '.field-name-field-writernote > .field-items > .field-item',
                 'article_attribution_not_verified')
    name = reporter.get_text(' ', strip=True)
    if source.get_text(' ', strip=True) != '苦勞報導' or source.get('href') != '/story':
        raise NewsError('article_rights_or_scope_excluded')
    if (not name or reporter.get('href') != '/author/' + quote(name, safe='')
            or staff.get_text(' ', strip=True) != '苦勞網記者'):
        raise NewsError('article_attribution_not_verified')
    editor = _one(article, ':scope > .field-name-field-chiefeditor', 'article_attribution_not_verified')
    label = _one(editor, ':scope > .field-label', 'article_attribution_not_verified')
    editor_name = _one(editor, ':scope > .field-items > .field-item > a[href]', 'article_attribution_not_verified')
    chief = editor_name.get_text(' ', strip=True)
    if (label.get_text(' ', strip=True) != '責任主編:' or not chief
            or editor_name.get('href') != '/chiefeditor/' + quote(chief, safe='')):
        raise NewsError('article_attribution_not_verified')
    clocks = {}
    for role in ('published', 'modified'):
        raw = _one(soup, f'head meta[property="article:{role}_time"]',
                   'invalid_bulk_publication_clock').get('content', '')
        try:
            if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}', raw):
                raise ValueError('unreviewed precision')
            clocks[role] = (raw, datetime.fromisoformat(raw))
        except ValueError:
            raise NewsError('invalid_bulk_publication_clock') from None
    day = _one(info, '.field-name-field-actdate .date-display-single', 'invalid_bulk_publication_clock')
    day_raw = day.get_text(' ', strip=True)
    parsed_day = _release_day(day_raw, r'[0-9]{4}/[0-9]{2}/[0-9]{2}', '%Y/%m/%d')
    if (parsed_day != clocks['published'][1].date() or clocks['modified'][1] < clocks['published'][1]
            or _one(soup, 'head meta[property="og:updated_time"]', 'invalid_bulk_publication_clock').get('content')
            != clocks['modified'][0]):
        raise NewsError('invalid_bulk_publication_clock')
    copyright_links = [n for n in soup.select('a[href="/node/4524"]') if n.get_text(' ', strip=True) == '版權宣告']
    if len(copyright_links) != 1:
        raise NewsError('article_license_not_verified')
    body = _one(article, ':scope > .field-name-body > .field-items > .field-item')
    for node in (page, region, article, heading, native_p, info, source, credit, reporter, staff,
                 editor, label, editor_name, day, copyright_links[0], body):
        _visible(node)
    for root in (native_p, info, credit, editor, body):
        for node in root.find_all(True):
            if node.name not in {'img', 'figure', 'figcaption'} and not node.find_parent(['figure', 'figcaption']):
                _visible(node)
    if body.select('form, [data-paywall], .paywall, [rel~="license"]'):
        raise NewsError('article_rights_or_scope_excluded')
    return {
        'headline': title, 'author': [{'@type': 'Person', 'name': name}],
        'author_attribution_detail_json': json.dumps({'reporter_raw': name, 'reporter_profile_raw': reporter['href'],
            'staff_role_raw': staff.get_text(' ', strip=True), 'chief_editor_raw': chief,
            'chief_editor_profile_raw': editor_name['href'], 'chief_editor_role_raw': label.get_text(' ', strip=True),
            'original_source_label_raw': source.get_text(' ', strip=True),
            'calendar_field_actdate_raw': day_raw, 'calendar_field_used_as_publication': False,
            'editor_or_facebook_account_used_as_reporter': False}, ensure_ascii=False),
        'datePublished': clocks['published'][0], 'dateModified': clocks['modified'][0],
        'published_time_basis': 'current_article_published_time_exact_seconds_offset; calendar_field_day_agrees_but_is_not_publication',
        'article_identity_basis': 'native_node_ID_type_canonical_shortlink_og_visible_title_and_staff_source_v1',
        'verified_article_notices_json': json.dumps([{'kind': 'publisher_original_copyright_notice',
            'text': 'CC-BY-NC-SA-2.0-TW; applies to verified publisher original, not third-party archives or submissions',
            'source_links': [_COOLLOUD_COPYRIGHT], 'license_url': _COOLLOUD_LICENSE, 'image_sources': []}], ensure_ascii=False),
        'body_completeness': 'verified_current_native_publisher_original_prose; staff_editor_calendar_tags_and_media_excluded',
    }, article, 'publisher_original_bound_to_linked_CC-BY-NC-SA-2.0-TW_notice_v1'
